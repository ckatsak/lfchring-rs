use std::borrow::Borrow;
use std::fmt::{Display, Formatter, Write};
use std::hash::{Hash, Hasher as StdHasher};
use std::mem;
use std::sync::Arc;

use crate::types::{Hasher, Node, Vnid};

/// VirtualNode represents a single virtual node in the ring.
#[derive(Debug)]
pub struct VirtualNode<N>
where
    N: Node + ?Sized,
{
    pub(crate) name: Vec<u8>,
    pub(crate) node: Arc<N>,
    vnid: Vnid,
    pub(crate) replica_owners: Option<Vec<Arc<N>>>,
}

impl<N> VirtualNode<N>
where
    N: Node + ?Sized,
{
    pub(crate) fn new<H: Hasher>(hasher: &mut H, node: Arc<N>, vnid: Vnid) -> Self {
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
            // FIXME: Redundant enumeration, ugly output
            for (_, owner) in self.replica_owners.as_ref().unwrap().iter().enumerate() {
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
