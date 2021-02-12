//! TODO: Crate documentation
//!
//! - In multi-threaded context, `HashRing` should be explicitly wrapped in `Arc`. This is a
//! deliberate, to expose the hidden cost of atomic reference counting and also give a chance to
//! single-threaded contexts to opt out of it.

//#![deny(missing_docs)]
#![allow(dead_code, unused_variables, unused_imports)]

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::fmt::Write;
use std::hash::{Hash, Hasher as StdHasher};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::{self as epoch, Atomic, Owned};
//use itertools::Itertools;
use log::trace;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// Auxiliary Types
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

pub type VNID = u16;

pub type Result<T> = std::result::Result<T, HashRingError>;

// TODO: Reporting `VirtualNode`s as `String`s is probably useless. Is there any case where
// exposing actual information (e.g., some struct) to the caller through `HashRingError` could turn
// out to be useful?
#[derive(Debug, Error)]
pub enum HashRingError {
    #[error("Virtual node {0:?} is already in the ring")]
    VirtualNodeAlreadyPresent(String),
    #[error("Virtual node {0:?} is not in the ring")]
    VirtualNodeAbsent(String),
    #[error("Concurrent compare-and-swap modification detected")]
    ConcurrentModification,
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
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn get_name(&self) -> Cow<[u8]>;
}

/// VirtualNode represents a single virtual node in the ring.
#[derive(Debug)]
pub struct VirtualNode<N: Node + ?Sized> {
    name: Vec<u8>,
    node: Arc<N>,
    vnid: VNID,
}

impl<N: Node + ?Sized> VirtualNode<N> {
    fn new<H: Hasher>(hasher: &mut H, node: Arc<N>, vnid: VNID) -> Self {
        let node_name = node.get_name();
        let mut name = Vec::with_capacity(node_name.len() + mem::size_of::<VNID>());
        name.extend(&*node_name);
        name.extend(&vnid.to_ne_bytes());
        let name = hasher.digest(&name);
        VirtualNode { name, node, vnid }
    }
}

impl<N: Node + ?Sized> Clone for VirtualNode<N> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            node: Arc::clone(&self.node),
            vnid: self.vnid,
        }
    }
}

// Required for `Eq`.
impl<N: Node + ?Sized> PartialEq for VirtualNode<N> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

// Required for `Ord`.
impl<N: Node + ?Sized> Eq for VirtualNode<N> {}

// Required for `Ord`.
impl<N: Node + ?Sized> PartialOrd for VirtualNode<N> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

// `Ord` is required to be able to store `VirtualNode` in a `BTreeSet`. Ordering `VirtualNode`s
// should probably only depend on their `name`, therefore we implement it rather than derive it.
impl<N: Node + ?Sized> Ord for VirtualNode<N> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

// Hash `VirtualNode`s based on their `name` field only, because I think the following must
// *always* hold:
//      if (x == y) then (hash(x) == hash(y))
// It is also demonstrated here:
//      https://doc.rust-lang.org/std/collections/index.html#insert-and-complex-keys
impl<N: Node + ?Sized> Hash for VirtualNode<N> {
    fn hash<H: StdHasher>(&self, hasher: &mut H) {
        self.name.hash(hasher);
    }
}

impl<N: Node + ?Sized> std::fmt::Display for VirtualNode<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No allocations at all:
        //write!(
        //    f,
        //    "{:02x?} ({:02x?}-{})",
        //    self.name,
        //    self.node.get_name(),
        //    self.vnid
        //)

        // >=2 `String` allocations:
        let mut name_hex = String::with_capacity(2 * self.name.len());
        self.name
            .iter()
            .for_each(|byte| write!(name_hex, "{:02x}", byte).unwrap());
        let node = &self.node.get_name();
        let node = String::from_utf8_lossy(&node);
        write!(f, "{} ({}-{})", name_hex, node, self.vnid)
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
pub struct HashRing<N: Node + ?Sized, H: Hasher> {
    inner: Atomic<HashRingState<N, H>>,
}

impl<N: Node + ?Sized, H: Hasher> Clone for HashRing<N, H> {
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

impl<N: Node + ?Sized> HashRing<N, DefaultStdHasher> {
    /// Create a new `HashRing` configured with the given parameters. It uses a `Hasher` based on
    /// `std::collections::hash_map::DefaultHasher` and initially contains the `VirtualNode`s of
    /// the given `nodes`.
    #[inline]
    pub fn with_nodes(
        vnodes_per_node: VNID,
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
    pub fn new(vnodes_per_node: VNID, replication_factor: u8) -> Result<Self> {
        Self::with_hasher_and_nodes(
            DefaultStdHasher::default(),
            vnodes_per_node,
            replication_factor,
            &[],
        )
    }
}

impl<N: Node + ?Sized, H: Hasher> HashRing<N, H> {
    /// Creates a new `HashRing`, properly initialized based on the given parameters, including the
    /// given `Hasher`. TODO
    pub fn with_hasher_and_nodes(
        hasher: H,
        vnodes_per_node: VNID,
        replication_factor: u8,
        nodes: &[Arc<N>],
    ) -> Result<Self> {
        let mut inner = HashRingState::empty(hasher, vnodes_per_node, replication_factor);
        inner.insert(nodes)?;
        Ok(Self {
            inner: Atomic::new(inner),
        })
    }

    /// TODO: Should we get rid of the `Result`, since `HashRingState::insert()` cannot really fail
    /// if no nodes are supplied at all?
    #[inline]
    pub fn with_hasher(hasher: H, vnodes_per_node: VNID, replication_factor: u8) -> Result<Self> {
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

    pub fn insert(&self, nodes: &[Arc<N>]) -> Result<()> {
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
        // to the local copy of the inner state).
        new_inner.insert(nodes)?;
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
        // Flush to make the deferred execution of the destructor run as soon as possible.
        guard.flush();

        Ok(())
    }
}

//unsafe impl<N: Node + ?Sized, H: Hasher> Send for HashRing<N, H> {}
//unsafe impl<N: Node + ?Sized, H: Hasher> Sync for HashRing<N, H> {} // FIXME

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// HashRingState
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct HashRingState<N: Node + ?Sized, H: Hasher> {
    hasher: H,
    vnodes_per_node: VNID,
    replication_factor: u8,
    vnodes: BTreeSet<VirtualNode<N>>,
}

impl<N: Node + ?Sized, H: Hasher> Clone for HashRingState<N, H> {
    fn clone(&self) -> Self {
        Self {
            hasher: H::default(),
            vnodes_per_node: self.vnodes_per_node,
            replication_factor: self.replication_factor,
            vnodes: self.vnodes.clone(),
        }
    }
}

impl<N: Node + ?Sized, H: Hasher> HashRingState<N, H> {
    #[inline]
    fn empty(hasher: H, vnodes_per_node: VNID, replication_factor: u8) -> Self {
        Self {
            hasher,
            vnodes_per_node,
            replication_factor,
            vnodes: BTreeSet::new(),
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
                if self.vnodes.contains(&vn) || !new.insert(vn.clone()) {
                    // FIXME: How to avoid cloning the VirtualNode ^ but also be able to use it in:
                    return Err(HashRingError::VirtualNodeAlreadyPresent(format!("{}", vn)));
                }
                trace!("vnode '{}' has been included in the ring extension", vn);
            }
        }
        self.vnodes.extend(new);
        //self.fix_replica_owners();
        Ok(())
    }

    #[allow(dead_code)]
    #[deprecated]
    fn fix_replica_owners(&mut self) {
        //for (curr, owners) in self.vnodes.iter_mut() {
        //    //
        //}

        //for (i, (curr, owners)) in self.vnodes.iter_mut().enumerate() {
        //    for (next, _) in self
        //        .vnodes
        //        .iter()
        //        .cycle()
        //        .skip(i)
        //        .take(self.replication_factor as usize)
        //    {
        //        //
        //    }
        //}

        //for (curr, owners) in self.vnodes.iter_mut().multipeek() {
        //    // Push my own...
        //    owners.push(Arc::clone(&curr.node));
        //    // ...and then push the next k-1 too.
        //    for k in 0..self.replication_factor - 1 {
        //        //
        //    }
        //}

        // Apparently, Rust does not allow the use of `cycle()` on an `iter_mut()` because
        // `cycle()` requires `Clone`.
        // Therefore, the next line is rejected by the compiler, whereas the line after that is
        // accepted (where `iter()` is used instead of `iter_mut()`).
        // The plan was to create a mutable iterator over the key-value pairs of the `BTreeMap`
        // (where the keys are the `VirtualNode`s and the values are `Vec<Arc<N>>`, which represent
        // the nodes that hold replicas of the `VirtualNode` in the ring), transform it to a
        // `MultiPeek` (see crate `Itertools`), then create a `Cycle` from it, and use it to peek
        // the next `VirtualNode`s until either:
        //  - the next k-1 distinct nodes are determined, or
        //  - the cycle has been exhausted and I am back to current `VirtualNode`.
        // However, as a result of the above limitation, I cannot find any way to actually
        // construct the loop, as I did in the Go implementation.
        // To work around this, I will probably use a `BTreeSet` rather than a `BTreeMap` to store
        // the `VirtualNode`s of the ring, and probably construct the replica owners on the fly,
        // upon request, by creating a Cycle from an immutable `iter()`, and then cloning the
        // distinct nodes as they are found.
        //let iter = self.vnodes.iter_mut().enumerate().multipeek().cycle();
        //let iter = self.vnodes.iter().enumerate().multipeek().cycle();

        unimplemented!()
    }

    #[inline]
    fn len_nodes(&self) -> usize {
        self.vnodes.len() / self.vnodes_per_node as usize
    }

    #[inline]
    fn len_virtual_nodes(&self) -> usize {
        self.vnodes.len()
    }
}

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

    use std::collections::HashSet;
    use std::thread;
    use std::time::Duration;

    use log::{debug, error, trace, warn};
    use rand::prelude::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    impl Node for String {
        fn get_name(&self) -> Cow<[u8]> {
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

    impl Node for &str {
        fn get_name(&self) -> Cow<[u8]> {
            Cow::Borrowed(self.as_bytes())
        }
    }
    #[test]
    fn node_str() {
        let s1 = "Node1";
        let a1 = Arc::new(s1);
        let mut h = DefaultStdHasher::default();

        let vn1 = VirtualNode::new(&mut h, Arc::clone(&a1), 1);
        let vn2 = VirtualNode::new(&mut h, Arc::clone(&a1), 2);
        let vn3 = VirtualNode::new(&mut h, Arc::clone(&a1), 3);

        eprintln!("vn1 = {:?},\nvn2 = {:?},\nvn3 = {:?}", vn1, vn2, vn3);
    }

    #[test]
    fn new_ring() {
        const VNODES_PER_NODE: VNID = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes = vec![Arc::new("Node1"), Arc::new("Node2"), Arc::new("Node3")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);

        assert!(ring.is_ok());
        let ring = ring.unwrap();
        eprintln!("ring = {:#?}", ring);

        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());
    }

    #[test]
    fn new_ring_already_in() {
        const VNODES_PER_NODE: VNID = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes = vec![Arc::new("Node1"), Arc::new("Node1"), Arc::new("Node1")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);
        eprintln!("ring = {:#?}", ring);
        assert!(ring.is_err());
        //let ring = ring.unwrap();
        //eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        //eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());
    }

    #[test]
    fn test_insert_singlethr_01() -> Result<()> {
        const VNODES_PER_NODE: VNID = 4;
        const REPLICATION_FACTOR: u8 = 2;
        init();

        let nodes = vec![Arc::new("Node1"), Arc::new("Node2"), Arc::new("Node3")];
        let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes)?;
        eprintln!("ring = {:#?}", ring);
        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());

        ring.insert(&[Arc::new("Node11"), Arc::new("Node12")])?;
        eprintln!("ring = {:#?}", ring);
        eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());

        Ok(())
    }

    #[test]
    fn test_insert_multithr_01() -> Result<()> {
        const VNODES_PER_NODE: VNID = 4;
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

        Ok(())
    }
}
