use std::iter::FusedIterator;

use crossbeam_epoch::Shared;

use crate::{
    state::HashRingState,
    types::{Hasher, Node},
    vnode::VirtualNode,
};

pub struct Iter<'guard, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    inner_ptr: Shared<'guard, HashRingState<N, H>>,
    front: usize,
    back: usize,
}

impl<'guard, N, H> Iter<'guard, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    #[inline]
    pub(crate) fn new(inner_ptr: Shared<'guard, HashRingState<N, H>>, len: usize) -> Self {
        Iter {
            inner_ptr,
            front: 0,
            back: len,
        }
    }
}

impl<'guard, N, H> Iterator for Iter<'guard, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    type Item = &'guard VirtualNode<N>;

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

impl<'guard, N, H> DoubleEndedIterator for Iter<'guard, N, H>
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

impl<'guard, N, H> ExactSizeIterator for Iter<'guard, N, H>
where
    N: Node + ?Sized,
    H: Hasher,
{
    #[inline]
    fn len(&self) -> usize {
        self.back - self.front
    }
}

impl<'guard, N: Node + ?Sized, H: Hasher> FusedIterator for Iter<'guard, N, H> {}
