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

/// A lock-free consistent hashing ring entity, designed for frequent reads by multiple concurrent
/// readers and infrequent updates by *one* _single writer_ at a time.
///
/// It features efficient support for virtual ring nodes per distinct node, as well as
/// "automatically managed" data replication among the distinct node.
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
    /// Create a new `HashRing` configured with the given parameters. It uses a `Hasher` based on
    /// `std::collections::hash_map::DefaultHasher` and initially contains the `VirtualNode`s of
    /// the given `nodes`.
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

    /// Create a new `HashRing` configured with the given parameters. It uses a `Hasher` based on
    /// `std::collections::hash_map::DefaultHasher` and it is initially empty.
    ///
    /// TODO: Should we get rid of the `Result`, since `HashRingState::insert()` cannot really fail
    /// if no nodes are supplied at all?
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
    /// Creates a new `HashRing`, properly initialized based on the given parameters, including the
    /// given `Hasher`. TODO
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

    /// TODO: Should we get rid of the `Result`, since `HashRingState::insert()` cannot really fail
    /// if no nodes are supplied at all?
    #[inline]
    pub fn with_hasher(hasher: H, vnodes_per_node: Vnid, replication_factor: u8) -> Result<Self> {
        Self::with_hasher_and_nodes(hasher, vnodes_per_node, replication_factor, &[])
    }

    pub fn len_nodes(&self) -> usize {
        let guard = epoch::pin();
        let inner = self.inner.load(Ordering::Acquire, &guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` setting it, and is never set to null. Furthermore, it always uses
        // Acquire/Release orderings. FIXME?
        unsafe { inner.as_ref().expect("inner HashRingState is null!") }.len_nodes()
        //unsafe { inner.deref() }.len_nodes()
    }

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
        let old_inner = match self.inner.compare_and_set(
            curr_inner_ptr,
            new_inner_ptr,
            Ordering::AcqRel,
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

    #[inline]
    pub fn insert(&self, nodes: &[Arc<N>]) -> Result<()> {
        self.update(Update::Insert, nodes)
    }

    #[inline]
    pub fn remove(&self, nodes: &[Arc<N>]) -> Result<()> {
        self.update(Update::Remove, nodes)
    }

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

    // returns a clone of the `VirtualNode`
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

    #[inline]
    pub fn predecessor<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent(Adjacency::Predecessor, key)
    }

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

    pub fn predecessor_node<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent_node(Adjacency::Predecessor, key)
    }

    pub fn successor_node<K>(&self, key: &K) -> Result<VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        self.adjacent_node(Adjacency::Successor, key)
    }

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
    /// TODO: Documentation?
    ///
    /// Extend the `HashRing` by the `Node`s provided through the `Iterator` over `Arc<N>>`.
    ///
    /// Note that, due to the restriction of `Extend::extend()` method's signature, a `&mut
    /// HashRing` is required.
    /// The preferred way to extend the ring is via `HashRing::insert()` anyway; read the section
    /// below for further details.
    ///
    /// # Panics
    ///
    /// Although the `Extend` trait is implemented for `HashRing`, it is not the preferred way to
    /// extend it.
    /// The signature of `Extend::extend()` does not allow to return an `Err` if the extension
    /// fails.
    /// Therefore, in case of hash collision (e.g., when inserting an already existing `Node` in
    /// the `HashRing`) this method fails by panicking (although the ring remains in a consistent
    /// state, since updating the ring is considered an atomic operation).
    ///
    /// The preferred way to add nodes to the `HashRing` is via `HashRing::insert()` instead, which
    /// returns an `Err` that can be handled in case of a failure.
    /// Only use this method if you know for sure that hash collisions are extremely unlikely and
    /// practically impossible (e.g., when employing a cryptographically secure hash function).
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
