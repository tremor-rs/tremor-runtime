// Copyright 2022, The Tremor Team
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

//!
//! ::: note
//!    Authentication happens over the [GCP autentication](./index.md#GCP)
//! :::
//!
//! # `gpubsub_consumer`
//!
//! This connector allows consuming a Google `PubSub` queue.
//!
//! ## Configuration
//!
//! | option                 | required? | description                                                                                                                  |
//! |----------------------------|-----------|--------------------------------------------------------------------------------------------------------------------------|
//! | `subscription_id`          | yes       | ID of the subscription to use in the form `projects/my_project/subscriptions/test-subscription-a`                        |
//! | `connect_timeout`          | no        | Connection timeout in nanoseconds, defaults to 1 second                                                                  |
//! | `ack_deadline`             | no        | ACK deadline in nanoseconds, defaults to 10 seconds. `PubSub` will resend the message if it's not acked within this time |
//! | `max_outstanding_messages` | no        | The maximum number of messages to keep outstanding at any given time, defaults to 128                                    |
//! | `max_outstanding_bytes`    | no        | The maximum number of bytes to keep outstanding at any given time, defaults to 10MB                                      |
//! | `token`                    | yes       | The authentication token see [GCP autentication](./index.md#GCP)                                                         |
//! | `url`                      | no        | The endpoint for the `PubSub` API                                                                                        |
//!
//! ```tremor title="config.troy"
//! define connector gsub from gpubsub_consumer
//! with
//!     codec = "string",
//!     config = {
//!         "subscription_id": "projects/my_project/subscriptions/test-subscription-a",
//!         "token": "env", # required  - The GCP token to use for authentication, see [GCP authentication](./index.md#GCP)
//!     }
//! end;
//! ```
//!
//! ## Metadata
//!
//! The connector will set the `$pubsub_connector` metadata variable, which is a dictionary of the messages metadata.
//!
//! | field          | type                      | description                                                                                 |
//! |----------------|---------------------------|---------------------------------------------------------------------------------------------|
//! | `message_id`   | string                    | The ID of the message, as provided by `PubSub`                                              |
//! | `ordering_key` | string                    | The ordering key of the message                                                             |
//! | `publish_time` | integer                   | The time when the message was published (as nanoseconds since 1st January 1970 00:00:00 UTC |
//! | `attributes`   | record with string values | The attributes attached to the message                                                      |
//!
//! ## Payload structure
//! The raw payload will be passed as is to the codec
//!
//!
//! # `gpubsub_producer`
//!
//! This connector allows producing to a Google `PubSub` queue.
//!
//! ## Configuration
//!
//! | option            | required? | description                                                                             |
//! |-------------------|-----------|-----------------------------------------------------------------------------------------|
//! | `topic`           | yes       | The identifier of the topic, in the format of `projects/PROJECT_NAME/topics/TOPIC_NAME` |
//! | `connect_timeout` | no        | Connection timeout in nanoseconds                                                       |
//! | `request_timeout` | no        | Request timeout in nanoseconds                                                          |
//! | `url`             | no        | The endpoint for the `PubSub` API                                                       |
//! | `token`           | yes       | The authentication token see [GCP autentication](./index.md#GCP)                        |
//!
//! ```tremor title="config.troy"
//! define flow gbqtest
//! flow
//!     use std::time::nanos;
//!     
//!     define pipeline passthrough
//!     pipeline
//!         select event from in into out;
//!     end;
//!
//!     define connector metro from metronome
//!     with
//!         config = {"interval": nanos::from_seconds(1) }
//!     end;
//!
//!     define connector gpub from gpubsub_producer
//!     with
//!         codec = "json",
//!         config = {
//!             "topic": "projects/xxx/topics/test-topic-a", # required - the identifier of the topic
//!             "connect_timeout": nanos::from_seconds(1), # optional - connection timeout (nanoseconds) - defaults to 10s
//!             "request_timeout": nanos::from_seconds(10), # optional - timeout for each request (nanoseconds) - defaults to 1s
//!             "token": "env", # required  - The GCP token to use for authentication, see [GCP authentication](./index.md#GCP)
//!             "url":  "https://us-east1-pubsub.googleapis.com" # optional - the endpoint for the PubSub API, defaults to https://pubsub.googleapis.com
//!         }
//!     end;
//!
//!     create connector gpub;
//!     create connector metro;
//!
//!     create pipeline passthrough;
//!
//!     connect /connector/metro/out to /pipeline/passthrough;
//!     connect /pipeline/passthrough to /connector/gpub/in;
//! end;
//!
//! deploy flow gbqtest;
//! ```
//!
//! ## Metadata
//! The connector will use the `$gpubsub_producer` metadata variable, which can be used to set the `ordering_key`.
//!
//! | field          | type   | description                     |
//! |----------------|--------|---------------------------------|
//! | `ordering_key` | string | The ordering key of the message |
//!
//! ## Payload structure
//! The raw payload will be passed as is to the codec

pub(crate) mod consumer;
pub(crate) mod producer;
use crate::connectors::prelude::*;

#[allow(clippy::unwrap_used)]
fn default_endpoint() -> Url<HttpsDefaults> {
    // ALLOW: this URL is hardcoded, so the only reason for parse failing would be if it was changed
    Url::parse("https://pubsub.googleapis.com").unwrap()
}
fn default_connect_timeout() -> u64 {
    1_000_000_000u64 // 1 second
}
fn default_request_timeout() -> u64 {
    10_000_000_000u64 // 10 seconds
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[test]
    pub fn default_connect_timeout_is_1s() {
        let actual = Duration::from_nanos(default_connect_timeout());

        assert_eq!(actual, Duration::from_secs(1));
    }
}
