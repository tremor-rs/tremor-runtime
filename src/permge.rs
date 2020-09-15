// Copyright 2018-2020, Wayfair GmbH
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
#![allow(dead_code)]
// Based on merge from async_std
use core::pin::Pin;
use core::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::pipeline::{CfMsg, MgmtMsg, Msg};
use async_std::stream::Fuse;
use async_std::stream::Stream;
use async_std::stream::StreamExt;

pin_project! {
    /// A stream that merges two other streams into a single stream.
    ///
    /// This `struct` is created by the [`merge`] method on [`Stream`]. See its
    /// documentation for more.
    ///
    /// [`merge`]: trait.Stream.html#method.merge
    /// [`Stream`]: trait.Stream.html
    #[cfg_attr(feature = "docs", doc(cfg(unstable)))]
    #[derive(Debug)]
    pub struct PriorityMerge<High, Low> {
        #[pin]
        high: Fuse<High>,
        #[pin]
        low: Fuse<Low>,
    }
}

impl<High: Stream, Low: Stream> PriorityMerge<High, Low> {
    pub(crate) fn new(high: High, low: Low) -> Self {
        Self {
            high: high.fuse(),
            low: low.fuse(),
        }
    }
}

impl<High, Low, T> Stream for PriorityMerge<High, Low>
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

#[derive(Debug)]
pub(crate) enum M {
    F(Msg),
    C(CfMsg),
    M(MgmtMsg),
}

impl M {
    pub(crate) fn msg_type(&self) -> &'static str {
        match self {
            M::F(_) => "fwd",
            M::C(_) => "cf",
            M::M(_) => "mgmt",
        }
    }
}
