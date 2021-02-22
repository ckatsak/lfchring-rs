//#![deny(missing_docs)]
//#![deny(missing_doc_code_examples)]
#![allow(dead_code, unused_variables, unused_imports)]

use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::mem;
use std::sync::Arc;

use log::trace;

use crate::{
    types::{Adjacency, HashRingError, Hasher, Node, Result, Update, Vnid},
    vnode::VirtualNode,
};

#[derive(Debug)]
pub(crate) struct HashRingState<N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    hasher: H,
    vnodes_per_node: Vnid,
    replication_factor: u8,
    pub(crate) vnodes: Vec<VirtualNode<N>>,
    // `crate::iter::Iter` requires access to this field, hence the `pub(crate)`.
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
    pub(crate) fn with_capacity(
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
    pub(crate) fn insert(&mut self, nodes: &[Arc<N>]) -> Result<()> {
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
                //trace!("vnode '{}' has been included in the ring extension", vn);
            }
        }
        // TODO: What happens with the reallocation here? It is completely uncontrolled for now.
        self.vnodes.extend(new);
        self.vnodes.sort_unstable();
        self.fix_replica_owners();
        Ok(())
    }

    pub(crate) fn remove(&mut self, nodes: &[Arc<N>]) -> Result<()> {
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
                    //trace!("Removing vnode '{:x?}' at index {}.", vn, index);
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
    pub(crate) fn len_nodes(&self) -> usize {
        self.vnodes.len() / self.vnodes_per_node as usize
    }

    #[inline]
    pub(crate) fn len_virtual_nodes(&self) -> usize {
        self.vnodes.len()
    }

    pub(crate) fn has_virtual_node<K>(&self, key: &K) -> bool
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
    pub(crate) fn virtual_node_for_key<K>(&self, key: &K) -> Result<&VirtualNode<N>>
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

    pub(crate) fn adjacent<K>(&self, adjacency: Adjacency, key: &K) -> Result<&VirtualNode<N>>
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

    pub(crate) fn adjacent_node<K>(&self, adjacency: Adjacency, key: &K) -> Result<&VirtualNode<N>>
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
