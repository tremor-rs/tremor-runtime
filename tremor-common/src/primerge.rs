// Copyright 2020-2021, The Tremor Team
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

// Based on merge from async-std
use core::{
    pin::Pin,
    task::{Context, Poll},
};
use futures::{
    stream::{Fuse, Stream},
    StreamExt,
};
use pin_project_lite::pin_project;
use std::marker::PhantomData;

pin_project! {
    /// A stream that merges two other streams into a single stream.
    ///
    /// This `struct` is created by the [`merge`] method on [`Stream`]. See its
    /// documentation for more.
    ///
    /// [`merge`]: trait.Stream.html#method.merge
    /// [`Stream`]: trait.Stream.html
    #[derive(Debug)]
    pub struct PriorityMerge<T, High, Low> {
        #[pin]
        high: Fuse<High>,
        #[pin]
        low: Fuse<Low>,
        t: PhantomData<T>
    }
}

impl<T, High, Low> PriorityMerge<T, High, Low>
where
    High: Stream<Item = T>,
    Low: Stream<Item = T>,
{
    /// Creates a new `PriorityMerge` from two streams.
    pub fn new(high: High, low: Low) -> Self {
        Self {
            high: high.fuse(),
            low: low.fuse(),
            t: PhantomData,
        }
    }
}

impl<T, High, Low> Stream for PriorityMerge<T, High, Low>
where
    High: Stream<Item = T>,
    Low: Stream<Item = T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.high.poll_next(cx) {
            Poll::Ready(None) => this.low.poll_next(cx),
            Poll::Ready(item) => Poll::Ready(item),
            Poll::Pending => match this.low.poll_next(cx) {
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
                Poll::Ready(item) => Poll::Ready(item),
            },
        }
    }
}
