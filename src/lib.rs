//! TODO: Crate documentation

//#![deny(missing_docs)]
#![allow(dead_code, unused_variables, unused_imports)]

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::Hasher as StdHasher;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::Atomic;
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

pub type Result<T> = std::result::Result<T, CHRingError>;

#[derive(Debug, Error)]
pub enum CHRingError {
    #[error("Virtual node {vn:?} is already in the ring")]
    VirtualNodeAlreadyIn { vn: String },
    #[error("Virtual node {vn:?} is not in the ring")]
    VirtualNodeAbsent { vn: String },
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// Hasher
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait Hasher {
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8>;
}

#[derive(Debug)]
struct DefaultStdHasher(DefaultHasher);

impl DefaultStdHasher {
    fn new() -> Self {
        DefaultStdHasher(DefaultHasher::new())
    }
}

impl Hasher for DefaultStdHasher {
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8> {
        // FIXME: DefaultStdHasher does not need to carry a DefaultHasher since an instance of the
        // latter has to be instantiated anew every time to reset its internal state.
        self.0 = DefaultHasher::new();
        self.0.write(bytes);
        let digest = self.0.finish();
        digest.to_ne_bytes().to_vec()
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
///
/// FIXME: `Ord` is required for automatic derivation of `Ord` on `VirtualNode`. However, ordering
/// `VirtualNode`s should only depend on their `name`. `VirtualNode` lacks a custom `Ord`
/// implementation for now.
pub trait Node: Ord {
    /// Retrieve a name that uniquely identifies the particular Node.
    fn get_name(&self) -> Vec<u8>;
}

/// VirtualNode represents a single virtual node in the ring.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct VirtualNode<N: Node + ?Sized> {
    name: Vec<u8>,
    node: Arc<N>,
    vnid: VNID,
}

impl<N: Node + ?Sized> VirtualNode<N> {
    fn new<H: Hasher>(hasher: &mut H, node: Arc<N>, vnid: VNID) -> Self {
        let mut name = node.get_name();
        name.extend(&vnid.to_ne_bytes());
        let name = hasher.digest(&name);
        VirtualNode { name, node, vnid }
    }
}

//// Required for `Eq`.
//impl<N: Node + ?Sized> PartialEq for VirtualNode<N> {
//    fn eq(&self, other: &Self) -> bool {
//        self.name.eq(&other.name)
//    }
//}
//
//// Required for `Ord`.
//impl<N: Node + ?Sized> PartialOrd for VirtualNode<N> {
//    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//        self.name.partial_cmp(&other.name)
//    }
//}
//
//// Required for use in `BTreeMap`.
//impl<N: Node + ?Sized> Ord for VirtualNode<N> {
//    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//        self.name.cmp(&other.name)
//    }
//}

impl<N> std::fmt::Display for VirtualNode<N>
where
    N: Node + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x?} ({:02x?}-{})",
            self.name,
            self.node.get_name(),
            self.vnid
        )
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
    state: Atomic<HashRingState<N, H>>,
}

impl<N: Node + ?Sized, H: Hasher> HashRing<N, H> {
    /// Creates a new `HashRing`, properly initialized based on the given parameters, including the
    /// given `Hasher`.
    pub fn with_hasher(
        hasher: H,
        vnodes_per_node: VNID,
        replication_factor: u8,
        nodes: &[Arc<N>],
    ) -> Result<Self> {
        let mut state = HashRingState::empty(hasher, vnodes_per_node, replication_factor);
        //let mut state = HashRingState::empty(hasher, vnodes_per_node, replication_factor, nodes);
        state.insert(nodes)?;
        Ok(HashRing {
            state: Atomic::new(state),
        })
    }

    pub fn len_nodes(&self) -> usize {
        let guard = crossbeam_epoch::pin();
        let state = self.state.load(Ordering::Acquire, &guard);
        unsafe { state.deref().len_nodes() }
    }

    pub fn len_virtual_nodes(&self) -> usize {
        let guard = crossbeam_epoch::pin();
        let state = self.state.load(Ordering::Acquire, &guard);
        unsafe { state.deref().len_virtual_nodes() }
    }
}

impl<N: Node + ?Sized> HashRing<N, DefaultStdHasher> {
    /// Creates a new `HashRing`, properly initialized based on the given parameters, using the
    /// default `Hasher` implementation, which is merely a wrapper for
    /// `std::collections::hash_map::DefaultHasher`.
    #[inline]
    pub fn new(vnodes_per_node: VNID, replication_factor: u8, nodes: &[Arc<N>]) -> Result<Self> {
        HashRing::with_hasher(
            DefaultStdHasher::new(),
            vnodes_per_node,
            replication_factor,
            nodes,
        )
    }
}

unsafe impl<N: Node + ?Sized, H: Hasher> Send for HashRing<N, H> {}
unsafe impl<N: Node + ?Sized, H: Hasher> Sync for HashRing<N, H> {} // FIXME

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
    vnodes: BTreeMap<VirtualNode<N>, Vec<Arc<N>>>,
}

impl<N: Node + ?Sized, H: Hasher> HashRingState<N, H> {
    #[inline]
    fn empty(
        hasher: H,
        vnodes_per_node: VNID,
        replication_factor: u8,
        //nodes: &[Arc<N>],
    ) -> Self {
        HashRingState {
            hasher,
            vnodes_per_node,
            replication_factor,
            vnodes: BTreeMap::new(),
        }
    }

    /// First, initialize all vnodes for the given nodes into a new `BTreeMap`. Then, check whether
    /// any of them is already present in the current vnodes map to make sure no collision occurs.
    /// Finally, merge the new vnodes into the old ones.
    //fn insert(&mut self, nodes: &[Arc<N>]) -> Result<BTreeMap<VirtualNode<N>, Vec<Arc<N>>>> {
    fn insert(&mut self, nodes: &[Arc<N>]) -> Result<()> {
        let mut new = BTreeMap::new();
        for node in nodes {
            for vnid in 0..self.vnodes_per_node {
                let vn = VirtualNode::new(&mut self.hasher, Arc::clone(&node), vnid);
                // We need to not only check whether vn is already in the ring, but also whether
                // it is present among the vnodes we are about to extend the ring by.
                if self.vnodes.contains_key(&vn) || new.contains_key(&vn) {
                    return Err(CHRingError::VirtualNodeAlreadyIn {
                        vn: format!("{}", vn),
                    });
                }
                trace!("Including vnode '{}' in the ring extension!", vn);
                let _ = new.insert(vn, Vec::with_capacity(self.replication_factor as usize));
            }
        }
        self.vnodes.extend(new);
        //self.fix_replica_owners();
        Ok(())
    }

    #[allow(dead_code)]
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

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    impl Node for String {
        fn get_name(&self) -> Vec<u8> {
            self.clone().into_bytes()
        }
    }
    #[test]
    fn node_string() {
        let s1 = String::from("Node1");
        //let a1: Arc<dyn Node> = Arc::new(s1);
        let a1 = Arc::new(s1);
        let mut h = DefaultStdHasher::new();

        let vn1 = VirtualNode::new(&mut h, Arc::clone(&a1), 1);
        let vn2 = VirtualNode::new(&mut h, Arc::clone(&a1), 2);
        let vn3 = VirtualNode::new(&mut h, Arc::clone(&a1), 3);

        eprintln!("vn1 = {:?},\nvn2 = {:?},\nvn3 = {:?}", vn1, vn2, vn3);
    }

    impl Node for &str {
        fn get_name(&self) -> Vec<u8> {
            self.bytes().collect()
        }
    }
    #[test]
    fn node_str() {
        let s1 = "Node1";
        let a1 = Arc::new(s1);
        let mut h = DefaultStdHasher::new();

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
        let ring = HashRing::new(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);

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
        let ring = HashRing::new(VNODES_PER_NODE, REPLICATION_FACTOR, &nodes);
        eprintln!("ring = {:#?}", ring);
        assert!(ring.is_err());
        //let ring = ring.unwrap();
        //eprintln!("ring.len_nodes() = {:#?}", ring.len_nodes());
        //eprintln!("ring.len_virtual_nodes() = {:#?}", ring.len_virtual_nodes());
    }
}
