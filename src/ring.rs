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

use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use log::trace;

use crate::{
    iter::Iter,
    state::HashRingState,
    types::{Adjacency, DefaultStdHasher, HashRingError, Hasher, Node, Result, Update, Vnid},
    vnode::VirtualNode,
};

/// The consistent hashing ring data structure.
///
/// Users will probably interact with this crate mostly through this type, as it is central to its
/// API.
///
/// In multi-threaded contexts, it needs to be wrapped in [`Arc`].
///
/// To find out more general information regarding its use, refer to the crate-level documentation.
#[derive(Debug)]
pub struct HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    inner: Atomic<HashRingState<N, H>>,
}

impl<N, H> Clone for HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    fn clone(&self) -> Self {
        // Pin the current thread.
        let guard = epoch::pin();
        // Atomically load the pointer.
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // Dereference it.
        // SAFETY: Only `HashRing::new()`, `HashRing::insert()` and `HashRing::remove()` ever
        // modify the pointer, and none of them sets it to null.
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };
        // Clone the copy of the inner state and wrap it in a new `Atomic` and a new `HashRing`.
        Self {
            inner: Atomic::new(inner.clone()),
        }
    }
}

impl<N> HashRing<N, DefaultStdHasher>
where
    N: Node + ?Sized,
{
    /// Create a new [`HashRing<N, H>`] configured with the given parameters (i.e., the number of
    /// *virtual nodes* per ring node and the *replication factor*) and initialize it with the
    /// provided `Node`s (the ring will be populated by their [`VirtualNode`]s automatically).
    ///
    /// The new [`HashRing<N, H>`] will employ the built-in [`Hasher`] that is based on standard
    /// library's [`DefaultHasher`][DefaultHasher].
    ///
    /// # Errors
    ///
    /// - Returns [`HashRingError::InvalidConfiguration`] if either the number of virtual nodes per
    /// distinct ring node or the replication factor is `0`.
    ///
    /// - Returns [`HashRingError::VirtualNodeAlreadyExists`] in the case that a hash collision
    /// occurs while attempting to insert the given [`Node`]s in the new consistent hashing ring.
    /// The reason for this can be one of the following:
    ///   - the output of [`Node::hashring_node_id`] for two (or more) of the [`Node`]s provided
    ///   for insertion is equal;
    ///   - the provided [`Hasher`] produces equal hash digests for different outputs of
    ///   [`Node::hashring_node_id`] for two (or more) [`Node`]s among those provided for
    ///   insertion.
    ///
    ///
    ///  [DefaultHasher]: https://doc.rust-lang.org/std/collections/hash_map/struct.DefaultHasher.html
    #[inline]
    pub fn with_nodes(
        vnodes_per_node: Vnid,
        replication_factor: u8,
        nodes: &[Arc<N>],
    ) -> Result<Self> {
        Self::with_hasher_and_nodes(
            DefaultStdHasher::default(),
            vnodes_per_node,
            replication_factor,
            nodes,
        )
    }

    /// Create a new [`HashRing<N, H>`] configured with the given parameters (i.e., the number of
    /// *virtual nodes* per ring node and the *replication factor*), which is initially empty of
    /// `Node`s (and, of course, empty of [`VirtualNode`]s too).
    ///
    /// The new [`HashRing<N, H>`] will employ the built-in [`Hasher`] that is based on standard
    /// library's [`DefaultHasher`][DefaultHasher].
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::InvalidConfiguration`] if either the number of virtual nodes per
    /// distinct ring node or the replication factor is `0`.
    ///
    ///
    ///  [DefaultHasher]: https://doc.rust-lang.org/std/collections/hash_map/struct.DefaultHasher.html
    #[inline]
    pub fn new(vnodes_per_node: Vnid, replication_factor: u8) -> Result<Self> {
        Self::with_hasher_and_nodes(
            DefaultStdHasher::default(),
            vnodes_per_node,
            replication_factor,
            &[],
        )
    }
}

impl<N, H> HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    /// Create a new [`HashRing<N, H>`] configured with the given parameters (i.e., the number of
    /// *virtual nodes* per ring node and the *replication factor*) and initialize it with the
    /// provided `Node`s (the ring will be populated by their [`VirtualNode`]s automatically).
    ///
    /// The new [`HashRing<N, H>`] will employ the provided [`Hasher`] for placing the
    /// [`VirtualNode`]s on the consistent hashing ring.
    ///
    /// # Errors
    ///
    /// - Returns [`HashRingError::InvalidConfiguration`] if either the number of virtual nodes per
    /// distinct ring node or the replication factor is `0`.
    ///
    /// - Returns [`HashRingError::VirtualNodeAlreadyExists`] in the case that a hash collision
    /// occurs while attempting to insert the given [`Node`]s in the new consistent hashing ring.
    /// The reason for this can be one of the following:
    ///   - the output of [`Node::hashring_node_id`] for two (or more) of the [`Node`]s provided
    ///   for insertion is equal;
    ///   - the provided [`Hasher`] produces equal hash digests for different outputs of
    ///   [`Node::hashring_node_id`] for two (or more) [`Node`]s among those provided for
    ///   insertion.
    pub fn with_hasher_and_nodes(
        hasher: H,
        vnodes_per_node: Vnid,
        replication_factor: u8,
        nodes: &[Arc<N>],
    ) -> Result<Self> {
        if replication_factor == 0 || vnodes_per_node == 0 {
            return Err(HashRingError::InvalidConfiguration(
                replication_factor,
                vnodes_per_node,
            ));
        }
        let mut inner =
            HashRingState::with_capacity(nodes.len(), hasher, vnodes_per_node, replication_factor);
        inner.insert(nodes)?;
        Ok(Self {
            inner: Atomic::new(inner),
        })
    }

    /// Create a new [`HashRing<N, H>`] configured with the given parameters (i.e., the number of
    /// *virtual nodes* per ring node and the *replication factor*), which is initially empty of
    /// `Node`s (and, of course, empty of [`VirtualNode`]s too).
    ///
    /// The new [`HashRing<N, H>`] will employ the provided [`Hasher`] for placing the
    /// [`VirtualNode`]s on the consistent hashing ring.
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::InvalidConfiguration`] if either the number of virtual nodes per
    /// distinct ring node or the replication factor is `0`.
    #[inline]
    pub fn with_hasher(hasher: H, vnodes_per_node: Vnid, replication_factor: u8) -> Result<Self> {
        Self::with_hasher_and_nodes(hasher, vnodes_per_node, replication_factor, &[])
    }

    /// Returns the number of distinct ring nodes that currently populate the consistent hashing
    /// ring.
    pub fn len_nodes(&self) -> usize {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        unsafe { inner.as_ref().expect("inner HashRingState is null!") }.len_nodes()
        //unsafe { inner.deref() }.len_nodes()
    }

    /// Returns the number of *virtual nodes* that currently populate the consistent hashing ring.
    ///
    /// This should always be equal to the result of [`HashRing::len_nodes`] multiplied by the
    /// `vnodes_per_node` for a particular [`HashRing<N, H>`].
    pub fn len_virtual_nodes(&self) -> usize {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        unsafe { inner.as_ref().expect("inner HashRingState is null!") }.len_virtual_nodes()
        //unsafe { inner.deref() }.len_virtual_nodes()
    }

    fn update(&self, op: Update, nodes: &[Arc<N>]) -> Result<()> {
        // Pin current thread.
        let guard = epoch::pin();

        ///////////////////////////////////////////////////////////////////////////////////////////
        // Use the `Atomic` pointer to atomically load the pointee (i.e., the current inner state)
        // to clone it and then update it.
        // This is the READ part of the RCU technique.
        //let curr_inner = self.inner.load(Ordering::Acquire, &guard);
        // It should be non-null.
        //assert!(!curr_inner.is_null(), "old inner HashRingState was null!");

        // Dereference the atomically loaded `Shared` that points to the inner state to clone it.
        // SAFETY: TODO
        //let new_inner = unsafe { (curr_inner.deref()).clone() };
        ///////////////////////////////////////////////////////////////////////////////////////////

        // Atomically load the pointer and then dereference it to retrieve the pointee, in order to
        // be able to clone it and then update it.
        // This is the READ part of the RCU technique.
        // Using `Ordering::Acquire` we make sure that no reads or writes in the current thread can
        // be reordered before this load. All writes in other threads that release the same atomic
        // variable are visible in the current thread.
        let curr_inner_ptr = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always us
        // setting it, and we never set it to null. Furthermore, we always use Acquire/Release
        // orderings, and it is assumed that there is always a single thread setting this. FIXME?
        let curr_inner =
            unsafe { curr_inner_ptr.as_ref() }.expect("old inner HashRingState was null!");

        // Clone the current inner HashRingState. This is the COPY part of the RCU technique.
        let mut new_inner = curr_inner.clone();

        //
        //  vvv  TODO: Modify the local copy to prepare it for the UPDATE part  vvv
        //
        // Modify the local copy of the inner state as deemed necessary (i.e., insert the new Nodes
        // to the local copy of the inner state, or remove the provided old ones from it).
        match op {
            Update::Insert => new_inner.insert(nodes)?,
            Update::Remove => new_inner.remove(nodes)?,
        }
        let new_inner_ptr = Owned::new(new_inner);
        //
        //  ^^^  TODO: Modify the local copy to prepare it for the UPDATE part  ^^^
        //

        // Atomically overwrite the pointer to the inner state with a pointer to the new, updated
        // one.
        // This is the UPDATE part of the RCU technique.
        // Using `Ordering::AcqRel` we make sure that no memory reads or writes in the current
        // thread can be reordered before or after this store. All writes in other threads that
        // release the same atomic variable are visible before the modification and the
        // modification is visible in other threads that acquire the same atomic variable.
        ///////////////////////////////////////////////////////////////////////////////////////////
        //let old_inner = self
        //    .inner
        //    .swap(Owned::new(new_inner), Ordering::AcqRel, &guard);
        ///////////////////////////////////////////////////////////////////////////////////////////
        // We use `compare_and_set()` rather than `swap()` to detect any concurrent modification
        // (i.e., any modification made by another thread since we last loaded the current inner
        // state of the HashRing), to give the caller a chance to evaluate possible new options.
        let old_inner = match self.inner.compare_exchange(
            curr_inner_ptr,
            new_inner_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
            &guard,
        ) {
            Ok(_) => {
                // On success, I think `compare_and_set()` returns `new_inner_ptr` as `Shared`;
                // therefore, the pointer to the "old" inner state is probably `curr_inner_ptr`.
                curr_inner_ptr
            }
            Err(cas_err) => {
                trace!(
                    "CAS failed; current: {:?}; new: {:?}",
                    cas_err.current,
                    cas_err.new
                );
                return Err(HashRingError::ConcurrentModification);
            }
        };

        // Defer the destruction of the old inner state until there are no active (i.e., "pinned")
        // threads in the current global epoch.
        // XXX: How is "destruction" defined? A simple deallocation will not do; we must make sure
        // that `Drop::drop()` is run, since `HashRingState` contains `VirtualNode`s which contain
        // `Arc<Node>` that must be referenced counted correctly.
        //  - According to The Rust Book, `Drop::drop()` _is_ a destructor.
        //  - It looks like `Guard::(self: &Self, ptr: Shared<'_, T>)` gets the ownership of `ptr`
        //  and does nothing more, hence `drop()`ping it in the end.
        // Therefore, this should probably be fine...(?)
        // SAFETY: TODO
        unsafe {
            guard.defer_destroy(old_inner);
        }
        // Flush to make the deferred execution of the destructor run as soon as possible. FIXME?
        guard.flush();

        Ok(())
    }

    /// Insert the given [`Node`]s to the consistent hashing ring, thereby expanding it.
    ///
    /// # Errors
    ///
    /// [`HashRingError::VirtualNodeAlreadyExists`] is returned in the case of a hash collision
    /// while attempting to insert the given [`Node`]s in the consistent hashing ring.
    /// This can happen if:
    /// - the output of [`Node::hashring_node_id`] for one (or more) of the new [`Node`]s is equal
    /// to that of one of the `Node`s that already exist in the ring;
    /// - the output of [`Node::hashring_node_id`] for two (or more) of the new [`Node`]s is equal
    /// to each other;
    /// - the provided [`Hasher`] produces equal hash digests for different outputs of
    /// [`Node::hashring_node_id`] for two (or more) [`Node`]s among the new or the already
    /// existing ones.
    #[inline]
    pub fn insert(&self, nodes: &[Arc<N>]) -> Result<()> {
        self.update(Update::Insert, nodes)
    }

    /// Remove the given [`Node`]s from the consistent hashing ring, thereby shrinking it.
    ///
    /// # Errors
    ///
    /// [`HashRingError::VirtualNodeDoesNotExist`] is returned in the case that one of the
    /// [`Node`]s provided for removal does not currently exist in the consistent hashing ring.
    #[inline]
    pub fn remove(&self, nodes: &[Arc<N>]) -> Result<()> {
        self.update(Update::Remove, nodes)
    }

    /// Returns `true` if the provided `key` corresponds to an existing [`VirtualNode`] of some
    /// [`Node`] in the consistent hashing ring, or `false` otherwise.
    pub fn has_virtual_node<K>(&self, key: &K) -> bool
    where
        K: Borrow<[u8]>,
    {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };
        inner.has_virtual_node(key)
    }

    /// Look up in the consistent hashing ring and return a **clone** of the [`VirtualNode`] that
    /// the given `key` should be assigned on.
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    pub fn virtual_node_for_key<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };

        let vn = inner.virtual_node_for_key(key)?;
        let mut ret = vn.clone();
        ret.replica_owners = Some(
            vn.replica_owners
                .as_ref()
                .expect("Inconsistent access to VirtualNode detected! Please file a bug report.")
                .clone(),
        );
        Ok(ret)
    }

    /// Look up in the consistent hashing ring and return a [`Vec`] of [`Node`]s, each wrapped in
    /// an [`Arc`], on which a replica of the given `key` should be assigned on.
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    pub fn nodes_for_key<K>(&self, key: &K) -> Result<Vec<Arc<N>>>
    where
        K: Borrow<[u8]>,
    {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };

        let vn = inner.virtual_node_for_key(key)?;
        Ok(vn
            .replica_owners
            .as_ref()
            .expect("Inconsistent access to VirtualNode detected! Please file a bug report.")
            .clone())
    }

    fn adjacent<K>(&self, adjacency: Adjacency, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };

        let vn = inner.adjacent(adjacency, key)?;
        let mut ret = vn.clone();
        ret.replica_owners = Some(
            vn.replica_owners
                .as_ref()
                .expect("Inconsistent access to VirtualNode detected! Please file a bug report.")
                .clone(),
        );
        Ok(ret)
    }

    /// Look up in the consistent hashing ring and return the [`VirtualNode`] which is the
    /// predecessor of the one that the given `key` should be assigned on.
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    #[inline]
    pub fn predecessor<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent(Adjacency::Predecessor, key)
    }

    /// Look up in the consistent hashing ring and return the [`VirtualNode`] which is the
    /// successor of the one that the given `key` should be assigned on.
    ///
    /// # Errors
    ///
    /// Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    #[inline]
    pub fn successor<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent(Adjacency::Successor, key)
    }

    fn adjacent_node<K>(&self, adjacency: Adjacency, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };

        let vn = inner.adjacent_node(adjacency, key)?;
        let mut ret = vn.clone();
        ret.replica_owners = Some(
            vn.replica_owners
                .as_ref()
                .expect("Inconsistent access to VirtualNode detected! Please file a bug report.")
                .clone(),
        );
        Ok(ret)
    }

    /// Look up in the consistent hashing ring and return the first predecessor [`VirtualNode`] to
    /// the one that the given `key` should be assigned on, but which also belongs to a different
    /// distinct [`Node`] than the latter.
    ///
    /// # Errors
    ///
    /// - Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    /// - Returns [`HashRingError::SingleDistinctNodeRing`] if the consistent hashing ring
    /// currently consists of a single distinct node and therefore all [`VirtualNode`]s in the ring
    /// actually belong to the same [`Node`].
    pub fn predecessor_node<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent_node(Adjacency::Predecessor, key)
    }

    /// Look up in the consistent hashing ring and return the first successor [`VirtualNode`] to
    /// the one that the given `key` should be assigned on, but which also belongs to a different
    /// distinct [`Node`] than the latter.
    ///
    /// # Errors
    ///
    /// - Returns [`HashRingError::EmptyRing`] if the consistent hashing ring is currently empty of
    /// [`Node`]s and therefore the given `key` cannot be assigned to any [`VirtualNode`] (as none
    /// exists).
    /// - Returns [`HashRingError::SingleDistinctNodeRing`] if the consistent hashing ring
    /// currently consists of a single distinct node and therefore all [`VirtualNode`]s in the ring
    /// actually belong to the same [`Node`].
    pub fn successor_node<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent_node(Adjacency::Successor, key)
    }

    /// Returns an [`Iter`], i.e., an iterator to loop through all [`VirtualNode`]s that populate
    /// the consistent hashing ring.
    ///
    /// See the documentation of [`Iter`] for more information regarding its use.
    //
    //  TODO: Include an example for calling [`HashRing::iter`], both here and in the documentation
    //  of [`Iter`].
    //
    #[inline]
    pub fn iter<'guard>(&self, guard: &'guard Guard) -> Iter<'guard, N, H> {
        let inner_ptr = self.inner.load(Ordering::Acquire, guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` or `remove()` setting it, and is never set to null. Furthermore, it always
        // uses Acquire/Release orderings. FIXME?
        let inner = unsafe { inner_ptr.as_ref() }.expect("Iter's inner HashRingState is null!");
        Iter::new(inner_ptr, inner.len_virtual_nodes())
    }
}

impl<N, H> Extend<Arc<N>> for HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    /// Extend the [`HashRing<N, H>`] by the [`Node`]s provided through the given [`IntoIterator`]
    /// over `Arc<N>>`.
    ///
    /// Note that, due to the restriction of [`Extend::extend`]'s signature, a `&mut HashRing` is
    /// required to use this method.
    /// The preferred way to extend the ring is via [`HashRing::insert`] anyway; read the section
    /// below for further details.
    ///
    /// # Panics
    ///
    /// Although the [`Extend`] trait is implemented for [`HashRing<N, H>`], it is not the
    /// preferred way of extending it.
    /// The signature of [`Extend::extend`] does not allow to return a `Result::Err` if the
    /// extension attempt fails.
    /// Therefore, in case of hash collision (e.g., when inserting an already existing [`Node`] in
    /// the [`HashRing<N, H>`]) this method fails by panicking (although the ring remains in a
    /// consistent state, since updating the ring is considered an atomic operation).
    ///
    /// The preferred way to add [`Node`]s to the [`HashRing<N, H>`] is via [`HashRing::insert`]
    /// instead, which returns a `Result::Err` that can be handled in case of a failure.
    /// Only use this method if you know for sure that hash collisions are extremely unlikely and
    /// practically impossible (e.g., when employing a cryptographically secure hash algorithm and
    /// no attempts to re-insert existing [`Node`]s occur).
    fn extend<I: IntoIterator<Item = Arc<N>>>(&mut self, iter: I) {
        let mut nodes = vec![];
        for node in iter {
            nodes.push(Arc::clone(&node));
        }
        if let Err(err) = self.update(Update::Insert, &nodes) {
            panic!("Error inserting new nodes to the ring: {}", err);
        }
    }
}

impl<N, H> Display for HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        let inner = unsafe { inner.as_ref().expect("inner HashRingState is null!") };
        write!(f, "{}", inner)
    }
}
