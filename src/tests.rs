use super::*;

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::panic;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use hex_literal::hex;
use log::{debug, error, trace, warn};
use rand::prelude::*;

use crate::types::{DefaultStdHasher, Update};

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

impl Node for String {
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
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
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
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
    let _ = ring.virtual_node_for_key(key)?;

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
        let _ = ring.virtual_node_for_key(key)?;

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
    let _ = ring.virtual_node_for_key(key)?;

    let prev_key = keys.get(keys.len() - 2).unwrap();
    let prev_vn = ring.virtual_node_for_key(prev_key)?;
    let pred = ring.predecessor(key)?;
    assert_eq!(pred, prev_vn);

    let next_key = keys.first().unwrap();
    let next_vn = ring.virtual_node_for_key(next_key)?;
    let succ = ring.successor(key)?;
    assert_eq!(succ, next_vn);

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

    let guard = &pin();
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
        let guard = &pin();
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

    let guard = &pin();

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

    let guard = &pin();

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

    let guard = &pin();

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

#[test]
fn test_contention_multithr_01() -> Result<()> {
    const VNODES_PER_NODE: Vnid = 4;
    const REPLICATION_FACTOR: u8 = 3;
    init();

    let ring = HashRing::with_nodes(VNODES_PER_NODE, REPLICATION_FACTOR, &[])?;

    // NOTE: Setting these to 100x20 is painfully slow...
    // Number of iterations per thread
    const ITERS: usize = 50;
    // Number of threads
    const NUM_THREADS: usize = 10;

    // Closure to insert/remove a chunk of distinct nodes to/from the ring.
    let chunk_operation =
        |op: Update, tid: usize, ring: Arc<HashRing<String, DefaultStdHasher>>| {
            let mut completed_nodes = HashSet::new();
            for node_id in tid * ITERS..(tid + 1) * ITERS {
                // produce a new node...
                let n = Arc::new(format!("Node-{}", node_id));
                trace!("[{}] adding {:?}...", tid, n);
                // ...and insist inserting/removing it until we succeed.
                while let Err(err) = match op {
                    Update::Insert => ring.insert(&[Arc::clone(&n)]),
                    Update::Remove => ring.remove(&[Arc::clone(&n)]),
                } {
                    match err {
                        HashRingError::ConcurrentModification => {
                            trace!("[{}] failed on Node-{}: {:?}", tid, node_id, err);
                        }
                        _ => {
                            warn!("[{}] failed on Node-{}: {:?}", tid, node_id, err);
                        }
                    }
                }
                trace!("[{}] progressed on Node-{}", tid, node_id);
                let _ = completed_nodes.insert(node_id);
            }
            completed_nodes
        };

    // Wrap the ring in an Arc to clone it for each thread later.
    let ring = Arc::new(ring);

    //
    // Insert the nodes in the ring:
    //
    // Threads' handles
    let mut handles = Vec::with_capacity(NUM_THREADS);
    // Threads' outputs
    let mut sets = Vec::with_capacity(NUM_THREADS);

    for tid in 0..NUM_THREADS {
        // Clone the ring once for each thread...
        let r = Arc::clone(&ring);
        // ...and spawn each one of them...
        handles.push(thread::spawn(move || {
            chunk_operation(Update::Insert, tid, r)
        }));
    }
    // ...then wait for all of them to finish.
    for (tid, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(s) => {
                trace!("[main] thread {} was successfully joined", tid);
                assert_eq!(s.len(), ITERS);
                sets.push(s);
            }
            Err(err) => {
                error!("[main] error joining thread {}: {:?}", tid, err);
            }
        };
    }

    //
    // Spawn multiple reader threads playing around concurrently
    //
    // Threads' handles
    let mut handles = Vec::with_capacity(NUM_THREADS);
    for _ in 0..NUM_THREADS {
        let r = Arc::clone(&ring);
        handles.push(thread::spawn(move || {
            let guard = &pin();
            assert_eq!(
                r.iter(guard).count(),
                NUM_THREADS * ITERS * VNODES_PER_NODE as usize
            );
            assert_eq!(r.iter(guard).count(), r.len_virtual_nodes(),);
        }));
    }

    //
    // Verify the correctness of the ring on the main thread, concurrently to the readers.
    //
    // Their results must be disjoint...
    sets.iter().enumerate().for_each(|(i, si)| {
        sets.iter().enumerate().for_each(|(j, sj)| {
            if i == j {
                assert!(si.is_subset(sj) && si.is_superset(sj));
            } else {
                assert!(si.is_disjoint(sj));
            }
        });
    });
    // ...so create their union...
    let union: BTreeSet<_> = sets.iter().flatten().collect();
    assert_eq!(union.len(), NUM_THREADS * ITERS);
    // ...which should contain all numbers in `0..NUM_THREADS * ITERS` (i.e., the Node IDs).
    union
        .iter()
        .zip(0..NUM_THREADS * ITERS)
        .for_each(|(&&id, i)| {
            assert_eq!(id, i);
        });
    trace!("[main] {} distinct nodes have been inserted", union.len());
    trace!("[main] len_nodes() = {}", ring.len_nodes());
    trace!("[main] len_virtual_nodes() = {}", ring.len_virtual_nodes());
    assert_eq!(union.len(), ring.len_nodes());
    assert_eq!(
        union.len() * VNODES_PER_NODE as usize,
        ring.len_virtual_nodes()
    );
    trace!("[main] ring = {}", ring);

    // Join reader threads
    for (tid, handle) in handles.into_iter().enumerate() {
        if let Err(err) = handle.join() {
            error!("[main] error joining thread {}: {:?}", tid, err);
        }
    }

    //
    // Now remove the nodes from the ring:
    //
    // Threads' handles
    let mut handles = Vec::with_capacity(NUM_THREADS);
    // Threads' outputs
    let mut sets = Vec::with_capacity(NUM_THREADS);

    for tid in 0..NUM_THREADS {
        // Clone the ring once for each thread...
        let r = Arc::clone(&ring);
        // ...and spawn each one...
        handles.push(thread::spawn(move || {
            chunk_operation(Update::Remove, tid, r)
        }));
    }
    // ...then wait for all of them to finish.
    for (tid, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(s) => {
                trace!("[main] thread {} was successfully joined", tid);
                assert_eq!(s.len(), ITERS);
                sets.push(s);
            }
            Err(err) => {
                error!("[main] error joining thread {}: {:?}", tid, err);
            }
        };
    }
    assert_eq!(0, ring.len_virtual_nodes());
    assert_eq!(0, ring.len_nodes());
    trace!("[main] ring = {}", ring);

    Ok(())
}
