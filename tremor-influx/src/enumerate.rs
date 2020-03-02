// Copyright 2018-2020, Wayfair GmbH
//
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

use std::iter::Iterator;
use std::ops::{Add, AddAssign};

/// An copy of Enumerate that allows extracting the content.
#[derive(Clone, Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Enumerate<I> {
    iter: I,
    count: usize,
}
impl<I> Enumerate<I> {
    pub(crate) fn new(iter: I) -> Enumerate<I> {
        Enumerate { iter, count: 0 }
    }
    pub(crate) fn parts(self) -> (usize, I) {
        (self.count, self.iter)
    }
    pub(crate) fn current_count(&self) -> usize {
        self.count
    }
}

impl<I> Iterator for Enumerate<I>
where
    I: Iterator,
{
    type Item = (usize, <I as Iterator>::Item);

    /// # Overflow Behavior
    ///
    /// The method does no guarding against overflows, so enumerating more than
    /// `usize::MAX` elements either produces the wrong result or panics. If
    /// debug assertions are enabled, a panic is guaranteed.
    ///
    /// # Panics
    ///
    /// Might panic if the index of the element overflows a `usize`.
    #[inline]
    fn next(&mut self) -> Option<(usize, <I as Iterator>::Item)> {
        let a = self.iter.next()?;
        let i = self.count;
        // Possible undefined overflow.
        AddAssign::add_assign(&mut self.count, 1);
        Some((i, a))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<(usize, I::Item)> {
        let a = self.iter.nth(n)?;
        // Possible undefined overflow.
        let i = Add::add(self.count, n);
        self.count = Add::add(i, 1);
        Some((i, a))
    }

    #[inline]
    fn count(self) -> usize {
        self.iter.count()
    }

    #[inline]
    fn fold<Acc, Fold>(self, init: Acc, fold: Fold) -> Acc
    where
        Fold: FnMut(Acc, Self::Item) -> Acc,
    {
        #[inline]
        fn enumerate<T, Acc>(
            mut count: usize,
            mut fold: impl FnMut(Acc, (usize, T)) -> Acc,
        ) -> impl FnMut(Acc, T) -> Acc {
            move |acc, item| {
                let acc = fold(acc, (count, item));
                // Possible undefined overflow.
                AddAssign::add_assign(&mut count, 1);
                acc
            }
        }

        self.iter.fold(init, enumerate(self.count, fold))
    }
}
