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

use std::iter::FusedIterator;

use crossbeam_epoch::Shared;

use crate::{
    state::HashRingState,
    types::{Hasher, Node},
    vnode::VirtualNode,
};

/// An iterator over the [`VirtualNode`]s of a [`HashRing<N, H>`].
///
/// This type implements the [`Iterator`] trait, as well as the [`DoubleEndedIterator`],
/// [`ExactSizeIterator`] and [`FusedIterator`], hence also enabling traversals of the virtual
/// nodes of the consistent hashing ring in a reversed order, acquiring the iterator's length, etc.
///
/// [`Iter`] is constructed through calls to [`HashRing::iter`].
/// However, note that the [`IntoIterator`] trait is not implemented because of a deviation in the
/// method's signature:
///
/// [`Iter`] is generic over the lifetime of a [`Guard`], which is required to ensure that the
/// underlying pointer to the original data structure where the virtual nodes have been stored
/// since the time of the creation of the iterator is not being freed by some subsequent write
/// operation on the ring.
/// The [`Guard`] must be created by the user of the iterator and passed to the [`Iter`] through
/// the [`HashRing::iter`] call.
/// For further information regarding the memory management techniques employed by this crate,
/// please refer to the crate-level documentation, as well as the documentation of
/// [`crossbeam_epoch`].
//
//  TODO: Include an example for calling [`HashRing::iter`], both here and in the documentation of
//  [`HashRing::iter`].
//
///
/// Also note that for the same reasons as above, [`Iter`] **cannot** be [`Send`] or [`Sync`], as
/// the deallocation of the data pointed to internally by the [`Iter`] effectively awaits the
/// thread that constructed the [`Iter`] to get un[`pin`]ned.
///
///
///  [`HashRing<N, H>`]: struct.HashRing.html
///  [`HashRing::iter`]: struct.HashRing.html#method.iter
///  [`Guard`]: struct.Guard.html
///  [`pin`]: fn.pin.html
///  [`crossbeam_epoch`]: https://docs.rs/crossbeam-epoch/0.9.2/crossbeam_epoch/index.html
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
