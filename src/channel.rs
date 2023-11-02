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

pub(crate) type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub(crate) type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
pub(crate) type UnboundedSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
pub(crate) type UnboundedReceiver<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
pub(crate) type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
// pub(crate) type OneShotReceiver<T> = tokio::sync::oneshot::Receiver<T>;
pub(crate) use tokio::sync::mpsc::error::SendError;
pub(crate) use tokio::sync::mpsc::{channel as bounded, unbounded_channel as unbounded};
pub(crate) use tokio::sync::oneshot::channel as oneshot;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ChannelError {
    #[error("Could not send")]
    Send,
    #[error("Could not recv")]
    Recv,
    #[error("The channel is empty")]
    Empty,
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn send_e<T>(_: crate::channel::SendError<T>) -> anyhow::Error {
    ChannelError::Send.into()
}

pub(crate) fn empty_e() -> ChannelError {
    ChannelError::Empty
}
