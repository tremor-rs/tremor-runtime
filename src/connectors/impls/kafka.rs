// Copyright 2021, The Tremor Team
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
pub(crate) mod consumer;
pub(crate) mod producer;

use crate::errors::{Error, Kind as ErrorKind, Result};
use core::future::Future;
use futures::future;
use rdkafka::{error::KafkaError, util::AsyncRuntime};
use rdkafka_sys::RDKafkaErrorCode;
use std::time::{Duration, Instant};

pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        // This needs to be smol::spawn we can't use async_std::task::spawn
        smol::spawn(task).detach();
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        // This needs to be smol::Timer we can't use async_io::Timer
        futures::FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}

/// verify broker host:port pairs in kafka connector configs
fn verify_brokers(id: &str, brokers: &Vec<String>) -> Result<(String, Option<u16>)> {
    let mut first_broker: Option<(String, Option<u16>)> = None;
    for broker in brokers {
        match broker.split(':').collect::<Vec<_>>().as_slice() {
            [host] => {
                first_broker.get_or_insert_with(|| ((*host).to_string(), None));
            }
            [host, port] => {
                let port: u16 = port.parse().map_err(|_| {
                    Error::from(ErrorKind::InvalidConfiguration(
                        id.to_string(),
                        format!("Invalid broker: {}:{}", host, port),
                    ))
                })?;
                first_broker.get_or_insert_with(|| ((*host).to_string(), Some(port)));
            }
            b => {
                return Err(ErrorKind::InvalidConfiguration(
                    id.to_string(),
                    format!("Invalid broker: {}", b.join(":")),
                )
                .into())
            }
        }
    }
    first_broker.ok_or_else(|| {
        ErrorKind::InvalidConfiguration(id.to_string(), "Missing brokers.".to_string()).into()
    })
}

/// Returns `true` if the error denotes a failed connect attempt
/// for both consumer and producer
fn is_failed_connect_error(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::ClientConfig(_, _, _, _)
            | KafkaError::ClientCreation(_)
            // TODO: what else?
            | KafkaError::Global(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::AllBrokersDown)
    )
}
