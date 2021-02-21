//! TODO: Crate documentation
//!
//! - In multi-threaded context, `HashRing` should be explicitly wrapped in `Arc`. This is
//! deliberate, to expose the hidden cost of atomic reference counting to the callers and also give
//! a chance to single-threaded contexts to opt out of it.

//#![deny(missing_docs)]
#![allow(dead_code, unused_variables, unused_imports)]

use std::borrow::{Borrow, Cow};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter, Write};
use std::hash::{Hash, Hasher as StdHasher};
use std::iter::FusedIterator;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use log::trace;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// Auxiliary Types
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

pub type Vnid = u16;

pub type Result<T> = std::result::Result<T, HashRingError>;

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

enum Update {
    Insert,
    Remove,
}

enum Adjacency {
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
struct DefaultStdHasher;

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

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// VirtualNode
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

/// Node represents a single distinct node in the ring.
pub trait Node {
    /// Retrieve a name that uniquely identifies the particular Node.
    fn hashring_node_id(&self) -> Cow<[u8]>;
}

/// VirtualNode represents a single virtual node in the ring.
#[derive(Debug)]
pub struct VirtualNode<N>
where
    N: Node + ?Sized,
{
    name: Vec<u8>,
    node: Arc<N>,
    vnid: Vnid,
    replica_owners: Option<Vec<Arc<N>>>,
}

impl<N> VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn new<H: Hasher>(hasher: &mut H, node: Arc<N>, vnid: Vnid) -> Self {
        let node_name = node.hashring_node_id();
        let mut name = Vec::with_capacity(node_name.len() + mem::size_of::<Vnid>());
        name.extend(&*node_name);
        name.extend(&vnid.to_ne_bytes());
        let name = hasher.digest(&name);
        VirtualNode {
            name,
            node,
            vnid,
            replica_owners: None, // XXX: `replica_owners` still unpopulated!
        }
    }

    pub fn replica_owners(&self) -> &[Arc<N>] {
        // By the time the `VirtualNode` became publicly accessible, its `replica_owners` field
        // must had already been populated via a call to `HashRingState::fix_replica_owners()`.
        // Therefore, unwrapping should never fail here.
        &self
            .replica_owners
            .as_ref()
            .expect("Inconsistent access to VirtualNode detected! Please file a bug report.")
    }
}

impl<N> Clone for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            node: Arc::clone(&self.node),
            vnid: self.vnid,
            replica_owners: None, // XXX: `replica_owners` still unpopulated!
        }
    }
}

// Required for `Eq`.
impl<N> PartialEq for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

// Required for `Ord`.
impl<N> Eq for VirtualNode<N> where N: Node + ?Sized {}

// Required for `Ord`.
impl<N> PartialOrd for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

// `Ord` is required to be able to store `VirtualNode` in a `BTreeSet`. Ordering `VirtualNode`s
// should probably only depend on their `name`, therefore we implement it rather than derive it.
impl<N> Ord for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

// Hash `VirtualNode`s based on their `name` field only, because I think the following must
// *always* hold:
//      if (x == y) then (hash(x) == hash(y))
// It is also demonstrated here:
//      https://doc.rust-lang.org/std/collections/index.html#insert-and-complex-keys
impl<N> Hash for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn hash<H: StdHasher>(&self, hasher: &mut H) {
        self.name.hash(hasher);
    }
}

impl<N> Borrow<[u8]> for VirtualNode<N>
where
    N: Node + ?Sized,
{
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        &self.name[..]
    }
}

impl<N> AsRef<[u8]> for VirtualNode<N>
where
    N: Node + ?Sized,
{
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.name[..]
    }
}

impl<N> Display for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // No allocations at all:
        //write!(
        //    f,
        //    "{:02x?} ({:02x?}-{})",
        //    self.name,
        //    self.node.hashring_node_id(),
        //    self.vnid
        //)

        // >=2 `String` allocations:
        let mut name_hex = String::with_capacity(2 * self.name.len());
        self.name
            .iter()
            .for_each(|byte| write!(name_hex, "{:02x}", byte).unwrap());
        let node = &self.node.hashring_node_id();
        let node = String::from_utf8_lossy(&node);

        // Also display the replica owners, if they exist.
        // One extra `String` allocation per replica owner.
        write!(f, "{} ({}-{}) --> (", name_hex, node, self.vnid)?;
        if self.replica_owners.is_some() {
            for (i, owner) in self.replica_owners.as_ref().unwrap().iter().enumerate() {
                write!(
                    f,
                    "{}, ",
                    String::from_utf8_lossy(&owner.hashring_node_id())
                )?
            }
        } else {
            write!(f, "UNPOPULATED")?
        }
        write!(f, ")")
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// HashRing
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

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
        for node in iter {
            if let Err(err) = self.update(Update::Insert, &[Arc::clone(&node)]) {
                panic!("Error inserting new nodes to the ring: {}", err);
            }
        }
        //let nodes = iter.into_iter().cloned();
        //self.update(Update::Insert, nodes);
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

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// HashRingState
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct HashRingState<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    hasher: H,
    vnodes_per_node: Vnid,
    replication_factor: u8,
    vnodes: Vec<VirtualNode<N>>,
}

impl<N, H> Clone for HashRingState<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    fn clone(&self) -> Self {
        Self {
            hasher: H::default(),
            vnodes_per_node: self.vnodes_per_node,
            replication_factor: self.replication_factor,
            vnodes: self.vnodes.clone(),
        }
    }
}

impl<N, H> HashRingState<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    #[inline]
    fn with_capacity(
        capacity: usize,
        hasher: H,
        vnodes_per_node: Vnid,
        replication_factor: u8,
    ) -> Self {
        Self {
            hasher,
            vnodes_per_node,
            replication_factor,
            vnodes: Vec::with_capacity(capacity),
        }
    }

    /// First, initialize all vnodes for the given nodes into a new `BTreeSet`. Then, check whether
    /// any of them is already present in the current vnodes map to make sure no collision occurs.
    /// Finally, merge the new vnodes into the old ones.
    ///
    /// NOTE: If any of the newly created `VirtualNode`s collides with an already existing one,
    /// none of the new `nodes` is inserted in the ring.
    fn insert(&mut self, nodes: &[Arc<N>]) -> Result<()> {
        let mut new = BTreeSet::new();
        for node in nodes {
            for vnid in 0..self.vnodes_per_node {
                let vn = VirtualNode::new(&mut self.hasher, Arc::clone(&node), vnid);
                // We need to not only check whether vn is already in the ring, but also whether
                // it is present among the vnodes we are about to extend the ring by.
                if self.vnodes.binary_search(&vn).is_ok() || !new.insert(vn.clone()) {
                    // FIXME: How to avoid cloning the VirtualNode ^ but also be able to use it in:
                    return Err(HashRingError::VirtualNodeAlreadyExists(format!("{}", vn)));
                }
                trace!("vnode '{}' has been included in the ring extension", vn);
            }
        }
        // TODO: What happens with the reallocation here? It is completely uncontrolled for now.
        self.vnodes.extend(new);
        self.vnodes.sort_unstable();
        self.fix_replica_owners();
        Ok(())
    }

    fn remove(&mut self, nodes: &[Arc<N>]) -> Result<()> {
        let mut removed_indices = BTreeSet::new();
        let node_names = nodes
            .iter()
            .map(|node| node.hashring_node_id())
            .collect::<Vec<_>>();
        let max_name_len = node_names.iter().map(|name| name.len()).max().unwrap();

        let mut name = Vec::with_capacity(max_name_len + mem::size_of::<Vnid>());
        for node_name in node_names {
            for vnid in 0..self.vnodes_per_node {
                name.clear();
                name.extend(&*node_name);
                name.extend(&vnid.to_ne_bytes());
                let vn = self.hasher.digest(&name);
                if let Ok(index) = self.vnodes.binary_search_by(|e| e.name.cmp(&vn)) {
                    trace!("Removing vnode '{:x?}' at index {}.", vn, index);
                    removed_indices.insert(index);
                } else {
                    return Err(HashRingError::VirtualNodeDoesNotExist(format!("{:x?}", vn)));
                }
            }
        }

        // TODO: Return the removed vnodes or not? I guess it would be best if the output of
        //       `HashRing::remove` is consistent with the output of `HashRing::insert`.
        let mut removed_vnodes = Vec::with_capacity(removed_indices.len());
        // Indices must be visited in reverse (descending) order for the removal; otherwise, the
        // indices of the virtual nodes to be removed in `self.vnodes` become invalid as they are
        // all shifted towards the beginning of the vector on every removal.
        for &index in removed_indices.iter().rev() {
            let vn = self.vnodes.remove(index);
            removed_vnodes.push(vn);
        }
        //assert!(self.vnodes.is_sorted());
        self.fix_replica_owners();
        //Ok(removed_vnodes) TODO
        Ok(())
    }

    fn fix_replica_owners(&mut self) {
        for i in 0..self.vnodes.len() {
            // SAFETY: `i` is always in range `0..self.vnodes.len()`
            let curr_vn = unsafe { self.vnodes.get_unchecked(i) };

            let mut replica_owners = Vec::with_capacity(self.replication_factor as usize);
            // Some capacity might be wasted here  ^^  but we prefer it over reallocation.
            let original_owner = &curr_vn.node;
            replica_owners.push(Arc::clone(original_owner));

            // Number of subsequent replica-owning nodes remaining to be found
            let mut k = self.replication_factor - 1;

            for (j, vn) in self
                .vnodes
                .iter()
                .enumerate()
                .cycle()
                .skip((i + 1) % self.vnodes.len())
            {
                // If all replica owners for this vnode have been determined, break.
                // Similarly, if we wrapped around the ring back to ourselves, break, even if k > 0
                // (which would mean that replication_factor > # of distinct ring nodes).
                if k == 0 || j == i {
                    break;
                }
                // Since we want distinct nodes only in `replica_owners`, make sure `vn.node` is
                // not already in.
                let mut node_already_in = false;
                for node in &replica_owners {
                    if vn.node.hashring_node_id() == node.hashring_node_id() {
                        node_already_in = true;
                        break;
                    }
                }
                // If `vn.node` is not already in, get it in, and decrease the number of distinct
                // nodes remaining to be found.
                if !node_already_in {
                    replica_owners.push(Arc::clone(&vn.node));
                    k -= 1;
                }
            }

            // Store the replica owners we just found for the current vnode, in the current vnode.
            // SAFETY: `i` is always in range `0..self.vnodes.len()`
            let mut curr_vn = unsafe { self.vnodes.get_unchecked_mut(i) };
            curr_vn.replica_owners = Some(replica_owners);
        }
    }

    #[inline]
    fn len_nodes(&self) -> usize {
        self.vnodes.len() / self.vnodes_per_node as usize
    }

    #[inline]
    fn len_virtual_nodes(&self) -> usize {
        self.vnodes.len()
    }

    fn has_virtual_node<K>(&self, key: &K) -> bool
    where
        K: Borrow<[u8]>,
    {
        self.vnodes
            .binary_search_by(|vn| {
                let name: &[u8] = &vn.name;
                name.cmp(key.borrow())
            })
            .is_ok()
    }

    // returns a reference to the actual `VirtualNode` in `HashRingState.vnodes`
    fn virtual_node_for_key<K>(&self, key: &K) -> Result<&VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        // Return an error if the ring is empty...
        if self.vnodes.is_empty() {
            return Err(HashRingError::EmptyRing);
        }
        // ...otherwise find the correct index and return the associated vnode.
        let index = self
            .vnodes
            .binary_search_by(|vn| {
                let name: &[u8] = &vn.name;
                name.cmp(key.borrow())
            })
            .unwrap_or_else(|index| index)
            % self.vnodes.len();
        // SAFETY: The remainder of the above integer division is always a usize between `0` and
        //         `self.vnodes.len() - 1`, hence can be used as an index in `self.vnodes`.
        Ok(unsafe { self.vnodes.get_unchecked(index) })
    }

    fn adjacent<K>(&self, adjacency: Adjacency, key: &K) -> Result<&VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        // Return an error if the ring is empty...
        if self.vnodes.is_empty() {
            return Err(HashRingError::EmptyRing);
        }
        // ...otherwise find the current index...
        let index = self
            .vnodes
            .binary_search_by(|vn| {
                let name: &[u8] = &vn.name;
                name.cmp(key.borrow())
            })
            .unwrap_or_else(|index| index)
            % self.vnodes.len();
        // ...and return the adjacent one.
        let index = match adjacency {
            Adjacency::Predecessor => {
                if 0 == index {
                    self.vnodes.len() - 1
                } else {
                    index - 1
                }
            }
            Adjacency::Successor => (index + 1) % self.vnodes.len(),
        };
        // SAFETY: The value of the index always stays within the range `0` to
        //         `self.vnodes.len() - 1`, hence can be used as an index in `self.vnodes`.
        Ok(unsafe { self.vnodes.get_unchecked(index as usize) })
    }

    fn adjacent_node<K>(&self, adjacency: Adjacency, key: &K) -> Result<&VirtualNode<N>>
    where
        K: Borrow<[u8]>,
    {
        // Return an error if the ring is empty or has only one distinct node...
        match self.vnodes.len() / self.vnodes_per_node as usize {
            0 => {
                return Err(HashRingError::EmptyRing);
            }
            1 => {
                return Err(HashRingError::SingleDistinctNodeRing);
            }
            _ => (),
        };

        // ...otherwise find the current index...
        let index = self
            .vnodes
            .binary_search_by(|vn| {
                let name: &[u8] = &vn.name;
                name.cmp(key.borrow())
            })
            .unwrap_or_else(|index| index)
            % self.vnodes.len();
        // ...and linearly search the vnode from there.
        match adjacency {
            Adjacency::Predecessor => {
                let mut iter = self
                    .vnodes
                    .iter()
                    .rev()
                    .cycle()
                    .skip(self.vnodes.len() - index)
                    .skip_while(|&vn| {
                        trace!("checking {} ...", vn);
                        vn.node.hashring_node_id()
                            == unsafe { self.vnodes.get_unchecked(index) }
                                .node
                                .hashring_node_id()
                    });
                iter.next()
            }
            Adjacency::Successor => {
                let mut iter = self
                    .vnodes
                    .iter()
                    .cycle()
                    .skip((index + 1) % self.vnodes.len())
                    .skip_while(|&vn| {
                        trace!("checking {} ...", vn);
                        vn.node.hashring_node_id()
                            == unsafe { self.vnodes.get_unchecked(index) }
                                .node
                                .hashring_node_id()
                    });
                iter.next()
            }
        }
        .ok_or_else(|| unreachable!())
    }
}

impl<N, H> Display for HashRingState<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "HashRingState ({} nodes X {} virtual, replication factor = {}) {{",
            self.len_nodes(),
            self.vnodes_per_node,
            self.replication_factor
        )?;
        for (i, vn) in self.vnodes.iter().enumerate() {
            writeln!(f, "\t- ({:0>6})  {}", i, vn)?
        }
        writeln!(f, "}}")
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// Iterator
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

impl<N, H> HashRing<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    #[inline]
    fn iter<'g>(&self, guard: &'g Guard) -> Iter<'g, N, H> {
        let inner_ptr = self.inner.load(Ordering::Acquire, guard);
        // SAFETY: `self.inner` is not null because after its initialization, it is always
        // `insert()` or `remove()` setting it, and is never set to null. Furthermore, it always
        // uses Acquire/Release orderings. FIXME?
        let inner = unsafe { inner_ptr.as_ref() }.expect("Iter's inner HashRingState is null!");
        Iter {
            inner_ptr,
            front: 0,
            back: inner.len_virtual_nodes(),
        }
    }
}

pub struct Iter<'g, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    inner_ptr: Shared<'g, HashRingState<N, H>>,
    front: usize,
    back: usize,
}

impl<'g, N, H> Iterator for Iter<'g, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    type Item = &'g VirtualNode<N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            // SAFETY: `self.inner` is not null because after its initialization, it is always
            // `insert()` or `remove()` setting it, and is never set to null. Furthermore, it always
            // uses Acquire/Release orderings. FIXME?
            let inner =
                unsafe { self.inner_ptr.as_ref() }.expect("Iter's inner HashRingState is null!");
            self.front += 1;
            inner.vnodes.get(self.front - 1)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.back - self.front;
        (rem, Some(rem))
    }
}

impl<'g, N, H> DoubleEndedIterator for Iter<'g, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            // SAFETY: `self.inner` is not null because after its initialization, it is always
            // `insert()` or `remove()` setting it, and is never set to null. Furthermore, it always
            // uses Acquire/Release orderings. FIXME?
            let inner =
                unsafe { self.inner_ptr.as_ref() }.expect("Iter's inner HashRingState is null!");
            self.back -= 1;
            inner.vnodes.get(self.back)
        } else {
            None
        }
    }
}

impl<'g, N, H> ExactSizeIterator for Iter<'g, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    #[inline]
    fn len(&self) -> usize {
        self.back - self.front
    }
}

impl<'g, N: Node + ?Sized, H: Hasher> FusedIterator for Iter<'g, N, H> {}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// tests
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::panic;
    use std::thread;
    use std::time::Duration;

    use hex_literal::hex;
    use log::{debug, error, trace, warn};
    use rand::prelude::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    impl Node for String {
        fn hashring_node_id(&self) -> Cow<[u8]> {
            Cow::Borrowed(&self.as_bytes())
        }
    }

    #[test]
    fn node_string() {
        let s1 = String::from("Node1");
        //let a1: Arc<dyn Node> = Arc::new(s1);
        let a1 = Arc::new(s1);
        let mut h = DefaultStdHasher::default();

        let vn1 = VirtualNode::new(&mut h, Arc::clone(&a1), 1);
        let vn2 = VirtualNode::new(&mut h, Arc::clone(&a1), 2);
        let vn3 = VirtualNode::new(&mut h, Arc::clone(&a1), 3);

        eprintln!("vn1 = {:?},\nvn2 = {:?},\nvn3 = {:?}", vn1, vn2, vn3);
    }

    impl Node for str {
        fn hashring_node_id(&self) -> Cow<[u8]> {
            Cow::Borrowed(self.as_bytes())
        }
    }

    #[test]
    fn node_str() {
        let s1 = "Node1";
        let a1: Arc<str> = Arc::from(s1);
        let mut h = DefaultStdHasher::default();

        let vn1 = VirtualNode::new(&mut h, Arc::clone(&a1), 1);
        let vn2 = VirtualNode::new(&mut h, Arc::clone(&a1), 2);
        let vn3 = VirtualNode::new(&mut h, Arc::clone(&a1), 3);

        eprintln!("vn1 = {:?},\nvn2 = {:?},\nvn3 = {:?}", vn1, vn2, vn3);
    }

    #[test]
    fn new_ring() {
        const VNODES_PER_NODE: Vnid = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes: Vec<Arc<str>> = vec![Arc::from("Node1"), Arc::from("Node2"), Arc::from("Node3")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);

        assert!(ring.is_ok());
        let ring = ring.unwrap();
        eprintln!("ring = {:#?}", ring);

        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());
    }

    #[test]
    fn new_ring_already_in() {
        const VNODES_PER_NODE: Vnid = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes: Vec<Arc<str>> = vec![Arc::from("Node1"), Arc::from("Node1"), Arc::from("Node1")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);
        eprintln!("ring = {:#?}", ring);
        assert!(ring.is_err());
        //let ring = ring.unwrap();
        //eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        //eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());
    }

    #[test]
    fn test_insert_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes: Vec<Arc<str>> = vec![Arc::from("Node1"), Arc::from("Node2"), Arc::from("Node3")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes)?;
        eprintln!("ring = {:#?}", ring);
        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());

        ring.insert(&[Arc::from("Node11"), Arc::from("Node12")])?;
        eprintln!("ring = {:#?}", ring);
        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());

        Ok(())
    }

    #[test]
    fn test_insert_multithr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        const ITERS: usize = 1000;
        let rand_insertions = |ring: Arc<HashRing<String, DefaultStdHasher>>| {
            let mut r = rand::thread_rng();
            let mut inserted_nodes = HashSet::new();
            for _ in 0..ITERS {
                // sleep for random duration (ms)
                let sleep_dur = r.gen_range(50..100);
                thread::sleep(Duration::from_millis(sleep_dur));
                // produce a new node & attempt to insert it
                let node_id: usize = r.gen_range(0..3000);
                let n = Arc::new(format!("Node-{}", node_id));
                trace!("adding {:?}...", n);
                match ring.insert(&[n]) {
                    Ok(_) => {
                        let _ = inserted_nodes.insert(node_id);
                    }
                    Err(err) => match err {
                        HashRingError::ConcurrentModification => {
                            warn!("{:?}", err);
                        }
                        _ => {
                            error!("{:?}", err);
                        }
                    },
                };
            }
            inserted_nodes
        };

        // Wrap the ring in an Arc and clone it once for each thread.
        let ring = Arc::new(ring);
        let r1 = Arc::clone(&ring);
        let r2 = Arc::clone(&ring);
        // Spawn the two threads...
        let t1 = thread::spawn(move || rand_insertions(r1));
        let t2 = thread::spawn(move || rand_insertions(r2));
        // ...and wait for them to finish.
        let s1 = t1.join().unwrap();
        let s2 = t2.join().unwrap();

        // Their results must be disjoint...
        assert!(s1.is_disjoint(&s2));
        // ...so create their union.
        let union: BTreeSet<_> = s1.union(&s2).collect();
        assert_eq!(union.len(), s1.len() + s2.len());
        //debug!("Thread sets:\ns1 = {:?}\ns2 = {:?}", s1, s2);
        //debug!("s1 ⋃ s2 = {:?}", union);
        debug!("Thr1 successfully inserted {} distinct nodes.", s1.len());
        debug!("Thr2 successfully inserted {} distinct nodes.", s2.len());
        debug!("A total of {} distinct nodes were inserted.", union.len());
        debug!("ring.len_nodes() = {}", ring.len_nodes());
        debug!("ring.len_virtual_nodes() = {}", ring.len_virtual_nodes());
        assert_eq!(union.len(), ring.len_nodes());
        assert_eq!(
            union.len() * VNODES_PER_NODE as usize,
            ring.len_virtual_nodes()
        );

        debug!("Hash Ring String Representation:\n{}", ring);

        Ok(())
    }

    #[test]
    fn test_replf_gt_nodes() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 2;
        const REPLICATION_FACTOR: u8 = 4;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        const NUM_NODES: usize = 3;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }

        debug!("ring.len_nodes() = {}", ring.len_nodes());
        debug!("ring.len_virtual_nodes() = {}", ring.len_virtual_nodes());
        debug!("Hash Ring String Representation:\n{}", ring);

        assert_eq!(ring.len_nodes(), NUM_NODES);
        assert_eq!(
            ring.len_virtual_nodes(),
            NUM_NODES * VNODES_PER_NODE as usize
        );

        Ok(())
    }

    #[test]
    fn test_remove_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 6;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        debug!("Hash Ring String Representation:\n{}", ring);
        assert_eq!(ring.len_nodes(), NUM_NODES);
        assert_eq!(
            ring.len_virtual_nodes(),
            NUM_NODES * VNODES_PER_NODE as usize
        );

        // Remove the nodes one by one
        for node_id in 0..NUM_NODES {
            assert_eq!(ring.len_nodes(), NUM_NODES - node_id);
            assert_eq!(
                ring.len_virtual_nodes(),
                (NUM_NODES - node_id) * VNODES_PER_NODE as usize
            );

            let n = Arc::new(format!("Node-{}", node_id));
            ring.remove(&[n])?;
            debug!("Hash Ring String Representation:\n{}", ring);

            assert_eq!(ring.len_nodes(), NUM_NODES - (node_id + 1));
            assert_eq!(
                ring.len_virtual_nodes(),
                (NUM_NODES - (node_id + 1)) * VNODES_PER_NODE as usize
            );
        }

        // Remove random node from empty ring
        assert_eq!(0, ring.len_nodes());
        if let Ok(()) = ring.remove(&[Arc::from("Node-42".to_string())]) {
            panic!("Unexpectedly removed 'Node-42' successfully from an empty ring!");
        }

        Ok(())
    }

    #[test]
    fn test_has_virtual_node_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        //debug!("Hash Ring String Representation:\n{}", ring);
        assert_eq!(ring.len_nodes(), NUM_NODES);
        assert_eq!(
            ring.len_virtual_nodes(),
            NUM_NODES * VNODES_PER_NODE as usize
        );

        // Test `HashRing::has_virtual_node()`
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            for vnid in 0..VNODES_PER_NODE {
                // Assert existence based on `VirtualNode`
                let vn = VirtualNode::new(&mut DefaultStdHasher::default(), Arc::clone(&n), vnid);
                assert!(ring.has_virtual_node(&vn));

                // Assert existence based on a raw `&[u8]`
                let node_name = n.hashring_node_id();
                let mut name = Vec::with_capacity(node_name.len() + mem::size_of::<Vnid>());
                name.extend(&*node_name);
                name.extend(&vnid.to_ne_bytes());
                let name = DefaultStdHasher::default().digest(&name);
                assert!(ring.has_virtual_node(&name));
            }

            // Assert non-existence based on `VirtualNode`
            let vn = VirtualNode::new(
                &mut DefaultStdHasher::default(),
                Arc::clone(&n),
                VNODES_PER_NODE,
            );
            assert!(!ring.has_virtual_node(&vn));

            // Assert non-existence based on a raw `&[u8]`
            let node_name = n.hashring_node_id();
            let mut name = Vec::with_capacity(node_name.len() + mem::size_of::<Vnid>());
            name.extend(&*node_name);
            name.extend(&VNODES_PER_NODE.to_ne_bytes());
            let name = DefaultStdHasher::default().digest(&name);
            assert!(!ring.has_virtual_node(&name));
        }

        // Assert non-existence based on completely random raw `&[u8]`s
        for v in vec![vec![0u8, 1, 2, 3, 4, 5], vec![42u8; 142]].iter() {
            assert!(!ring.has_virtual_node(v));
        }

        Ok(())
    }

    #[test]
    fn test_virtual_node_for_key_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        assert_eq!(ring.len_nodes(), NUM_NODES);
        assert_eq!(
            ring.len_virtual_nodes(),
            NUM_NODES * VNODES_PER_NODE as usize
        );

        // Keys selected as `VirtualNode.name + 1`
        let keys = vec![
            hex!("232a8a941ee901c1"),
            hex!("324317a375aa4201"),
            hex!("4ff59699a3bacc04"),
            hex!("62338d102fd1edce"),
            hex!("6aad47fd1f3fc789"),
            hex!("728254115d9da0a8"),
            hex!("7cf6c43df9ff4b72"),
            hex!("a416af15b94f0122"),
            hex!("ab1c5045e605c275"),
            hex!("acec6c33d08ac530"),
            hex!("cbdaa742e68b020d"),
            hex!("ed59d86868c13210"),
        ];

        // Remove the nodes one by one, checking the keys' distribution every time
        for node_id in 0..NUM_NODES {
            assert_eq!(ring.len_nodes(), NUM_NODES - node_id);
            debug!("Hash Ring String Representation:\n{}", ring);

            for key in &keys {
                let vn = ring.virtual_node_for_key(key)?;
                eprintln!("Key {:x?} is assigned to vnode {}", key, vn);
            }

            // Remove one node
            let n = Arc::new(format!("Node-{}", node_id));
            ring.remove(&[n])?;
        }

        Ok(())
    }

    #[test]
    fn test_nodes_for_key_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }

        // Keys selected as `VirtualNode.name + 1`
        let keys = vec![
            hex!("232a8a941ee901c1"),
            hex!("324317a375aa4201"),
            hex!("4ff59699a3bacc04"),
            hex!("62338d102fd1edce"),
            hex!("6aad47fd1f3fc789"),
            hex!("728254115d9da0a8"),
            hex!("7cf6c43df9ff4b72"),
            hex!("a416af15b94f0122"),
            hex!("ab1c5045e605c275"),
            hex!("acec6c33d08ac530"),
            hex!("cbdaa742e68b020d"),
            hex!("ed59d86868c13210"),
        ];

        for key in keys {
            assert_eq!(
                ring.virtual_node_for_key(&key)?.replica_owners(),
                ring.nodes_for_key(&key)?
            );
        }

        Ok(())
    }

    #[test]
    fn test_adjacent_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }

        // Keys selected as `VirtualNode.name + 1`
        let keys = vec![
            hex!("232a8a941ee901c1"),
            hex!("324317a375aa4201"),
            hex!("4ff59699a3bacc04"),
            hex!("62338d102fd1edce"),
            hex!("6aad47fd1f3fc789"),
            hex!("728254115d9da0a8"),
            hex!("7cf6c43df9ff4b72"),
            hex!("a416af15b94f0122"),
            hex!("ab1c5045e605c275"),
            hex!("acec6c33d08ac530"),
            hex!("cbdaa742e68b020d"),
            hex!("ed59d86868c13210"),
        ];

        // Test for the first node
        let key = keys.first().unwrap();
        let vn = ring.virtual_node_for_key(key)?;

        let prev_key = keys.last().unwrap();
        let prev_vn = ring.virtual_node_for_key(prev_key)?;
        let pred = ring.predecessor(key)?;
        assert_eq!(pred, prev_vn);

        let next_key = keys.get(1).unwrap();
        let next_vn = ring.virtual_node_for_key(next_key)?;
        let succ = ring.successor(key)?;
        assert_eq!(succ, next_vn);

        // Test for all the intermediate nodes
        for i in 1..keys.len() - 1 {
            let key = keys.get(i).unwrap();
            let vn = ring.virtual_node_for_key(key)?;

            let prev_key = keys.get(i - 1).unwrap();
            let prev_vn = ring.virtual_node_for_key(prev_key)?;
            let pred = ring.predecessor(key)?;
            assert_eq!(pred, prev_vn);

            let next_key = keys.get(i + 1).unwrap();
            let next_vn = ring.virtual_node_for_key(next_key)?;
            let succ = ring.successor(key)?;
            assert_eq!(succ, next_vn);
        }

        // Test for the last node
        let key = keys.last().unwrap();
        let vn = ring.virtual_node_for_key(key)?;

        let prev_key = keys.get(keys.len() - 2).unwrap();
        let prev_vn = ring.virtual_node_for_key(prev_key)?;
        let pred = ring.predecessor(key)?;
        assert_eq!(pred, prev_vn);

        let next_key = keys.first().unwrap();
        let next_vn = ring.virtual_node_for_key(next_key)?;
        let succ = ring.successor(key)?;

        Ok(())
    }

    #[test]
    fn test_adjacent_node_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }

        // Keys selected as `VirtualNode.name + 1`
        let keys = vec![
            hex!("232a8a941ee901c1"),
            hex!("324317a375aa4201"),
            hex!("4ff59699a3bacc04"),
            hex!("62338d102fd1edce"),
            hex!("6aad47fd1f3fc789"),
            hex!("728254115d9da0a8"),
            hex!("7cf6c43df9ff4b72"),
            hex!("a416af15b94f0122"),
            hex!("ab1c5045e605c275"),
            hex!("acec6c33d08ac530"),
            hex!("cbdaa742e68b020d"),
            hex!("ed59d86868c13210"),
        ];

        trace!("ring = {}", ring);

        // Check successor_node
        debug!("Check successor_node()...");
        for key in &keys {
            trace!("\n--> key = {:x?}", key);
            let owners = ring.nodes_for_key(key)?;
            trace!("owners = {:?}", owners);
            let succ_vn = ring.successor_node(key)?;
            trace!("succ_vn = {}", succ_vn);

            assert_eq!(
                succ_vn.node.hashring_node_id(),
                owners.get(1).unwrap().hashring_node_id()
            );
        }

        // Check predecessor_node
        debug!("Check predecessor_node()...");
        for key in &keys {
            trace!("\n--> key = {:x?}", key);
            let vn = ring.virtual_node_for_key(key)?;
            trace!("vn = {}", vn);
            let pred_vn = ring.predecessor_node(key)?;
            trace!("pred_vn = {}", pred_vn);

            assert_eq!(pred_vn.replica_owners()[1], vn.node);
        }

        Ok(())
    }

    #[test]
    fn test_iter_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        debug!("ring: {}", ring);

        let guard = &epoch::pin();
        for vn in ring.iter(guard) {
            trace!("vn = {}", vn);
            trace!("vn.name = {:x?}", vn.name);
            trace!("vn.node = {}", vn.node);
            trace!("vn.replica_owners = {:?}", vn.replica_owners());
        }

        Ok(())
    }

    #[test]
    fn test_iter_multithr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        debug!("ring: {}", ring);

        let ring = Arc::new(ring);
        let r1 = Arc::clone(&ring);
        let r2 = Arc::clone(&ring);

        let t1 = thread::spawn(move || {
            trace!("START: r1.len_virtual_nodes() = {}", r1.len_virtual_nodes());
            thread::sleep(Duration::from_millis(100));
            const TOTAL_NODES: usize = 20;
            for node_id in NUM_NODES..TOTAL_NODES {
                // produce a new node & attempt to insert it
                let n = Arc::new(format!("Node-{}", node_id));
                //trace!("adding {:?}...", n);
                if let Err(err) = r1.insert(&[n]) {
                    match err {
                        HashRingError::ConcurrentModification => {
                            warn!("{:?}", err);
                        }
                        _ => {
                            error!("{:?}", err);
                        }
                    };
                };
            }
            trace!("END: r1.len_virtual_nodes() = {}", r1.len_virtual_nodes());
        });
        let t2 = thread::spawn(move || {
            debug!("START: r2.len_vnodes() = {}", r2.len_virtual_nodes());

            // Create the iterator before the other thread starts inserting nodes...
            let guard = &epoch::pin();
            let hashring_iter = r2.iter(guard).enumerate();

            // ...and sleep for a sec...
            thread::sleep(Duration::from_millis(1000));

            // ...then go through the previously constructed iterator...
            let mut count = 0;
            for (i, vn) in hashring_iter {
                debug!("ITERATION {}: {}", i, vn);
                count += 1;
            }

            // and assert that we did NOT iterate more than the initially constructed ring's size.
            assert_eq!(count, NUM_NODES * VNODES_PER_NODE as usize);

            // NOTE: `r2` still points to the ring which is being updated, so the number of virtual
            // nodes reported below should reflect the changes made through `r1`, but the iterator
            // has been working with a snapshot of the ring before `t1`'s updates occur.
            trace!("END: r2.len_virtual_nodes() = {}", r2.len_virtual_nodes());
            trace!("END: r2 = {}", r2);
        });

        // wait for the threads to finish
        t1.join().unwrap();
        t2.join().unwrap();
        //trace!("ring.len_virtual_nodes() = {}", ring.len_virtual_nodes());
        //trace!("ring = {}", ring);

        Ok(())
    }

    /// Test DoubleEndedIterator
    #[test]
    fn test_iter_singlethr_02() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        debug!("ring: {}", ring);

        let guard = &epoch::pin();

        // Use `std::iter::Iterator::rev()` to place all vnodes in a Vec in reverse order...
        let mut vns = Vec::with_capacity(NUM_NODES * VNODES_PER_NODE as usize);
        for vn in ring.iter(guard).rev() {
            trace!("vn = {}", vn);
            trace!("vn.name = {:x?}", vn.name);
            trace!("vn.node = {}", vn.node);
            trace!("vn.replica_owners = {:?}", vn.replica_owners());
            vns.push(vn);
        }
        // ...then verify the result by comparing the Vec to the normal Iterator.
        for (i, vn) in ring.iter(guard).enumerate() {
            trace!(
                "comparing vn-{} to vns[{}]",
                i,
                NUM_NODES * VNODES_PER_NODE as usize - i - 1
            );
            assert_eq!(
                &vn,
                vns.get(NUM_NODES * VNODES_PER_NODE as usize - i - 1)
                    .expect("OOPS")
            );
        }

        Ok(())
    }

    /// Test DoubleEndedIterator
    #[test]
    fn test_iter_singlethr_03() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        trace!("ring: {}", ring);

        let guard = &epoch::pin();

        // First create a mapping between each vnode and its index
        let mut m = HashMap::with_capacity(NUM_NODES * VNODES_PER_NODE as usize);
        for (i, vn) in ring.iter(guard).enumerate() {
            m.insert(vn, i);
        }

        // Construct the iterator before adding extra nodes...
        let mut iter = ring.iter(guard);

        // Add some extra nodes, which we do not expect to go through later
        for node_id in 100..100 + NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }

        // Now alternate between the forward and the backward iterator
        let mut times = 0;
        let mut front = true;
        loop {
            let vn = if front { iter.next() } else { iter.next_back() };
            if let Some(vn) = vn {
                trace!("(vnode {:2}) : {}", m.get(vn).unwrap(), vn);
            } else {
                break;
            }
            front = !front;
            times += 1;
        }
        assert_eq!(times, NUM_NODES * VNODES_PER_NODE as usize);

        Ok(())
    }

    /// Test size_hint() and ExactSizeIterator
    #[test]
    fn test_iter_singlethr_04() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 3;
        const REPLICATION_FACTOR: u8 = 3;
        init();

        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

        // Insert the nodes
        const NUM_NODES: usize = 4;
        for node_id in 0..NUM_NODES {
            let n = Arc::new(format!("Node-{}", node_id));
            ring.insert(&[n])?;
        }
        trace!("ring: {}", ring);

        let guard = &epoch::pin();

        // ExactSizeIterator::len()
        assert_eq!(ring.iter(guard).count(), ring.iter(guard).len());

        // Iterator::size_hint()
        let iter = ring.iter(guard);
        trace!("iter.size_hint() = {:?}", iter.size_hint());
        assert_eq!(
            iter.size_hint(),
            (
                NUM_NODES * VNODES_PER_NODE as usize,
                Some(NUM_NODES * VNODES_PER_NODE as usize)
            )
        );

        let iter = iter.filter(|&vn| vn.name.last() == Some(&42));
        trace!("iter.size_hint() = {:?}", iter.size_hint());
        assert_eq!(
            iter.size_hint(),
            (0, Some(NUM_NODES * VNODES_PER_NODE as usize))
        );

        let iter = iter.chain(ring.iter(guard));
        trace!("iter.size_hint() = {:?}", iter.size_hint());
        assert_eq!(
            iter.size_hint(),
            (
                NUM_NODES * VNODES_PER_NODE as usize,
                Some(2 * NUM_NODES * VNODES_PER_NODE as usize)
            )
        );

        // FusedIterator
        const ITERS: usize = 1000;
        let mut iter = ring.iter(guard);
        for _ in iter.by_ref() {}
        let mut nonez = Vec::with_capacity(ITERS);
        for _ in 0..ITERS {
            nonez.push(iter.next());
        }
        assert!(nonez.iter().all(|none| none.is_none()));
        trace!("all {} entries are None!", nonez.len());

        Ok(())
    }

    #[test]
    fn test_extend_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: Vnid = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        // Initialize a ring with 3 nodes
        let nodes: Vec<Arc<str>> = vec![Arc::from("Node1"), Arc::from("Node2"), Arc::from("Node3")];
        let mut ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes)?;
        trace!("ring = {}", ring);
        assert_eq!(ring.len_nodes(), nodes.len());
        assert_eq!(
            ring.len_virtual_nodes(),
            nodes.len() * VNODES_PER_NODE as usize
        );

        // Extend the ring by 2 nodes
        ring.extend(vec![Arc::from("Node11"), Arc::from("Node12")]);
        trace!("ring = {}", ring);
        assert_eq!(ring.len_nodes(), nodes.len() + 2);
        assert_eq!(
            ring.len_virtual_nodes(),
            (nodes.len() + 2) * VNODES_PER_NODE as usize
        );

        // Attempt to extend it by the same 2 nodes, and catch it panicking
        let res = std::panic::catch_unwind(move || {
            ring.extend(vec![Arc::from("Node11"), Arc::from("Node12")])
        });
        assert!(res.is_err());

        Ok(())
    }
}
