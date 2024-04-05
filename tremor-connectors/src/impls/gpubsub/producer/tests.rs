// Copyright 2024, The Tremor Team
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

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn is_not_auto_ack() {
    let mock = crate::utils::google::tests::gouth_token()
        .await
        .expect("Failed to get token");

    let sink = GpubSink {
        config: Config {
            token: mock.token_src(),
            connect_timeout: 0,
            request_timeout: 0,
            url: Url::default(),
            topic: String::new(),
        },
        hostname: String::new(),
        client: None,
    };

    assert!(!sink.auto_ack());
}

#[tokio::test(flavor = "multi_thread")]
async fn is_async() {
    let mock = crate::utils::google::tests::gouth_token()
        .await
        .expect("Failed to get token");

    let sink = GpubSink {
        config: Config {
            token: mock.token_src(),
            connect_timeout: 0,
            request_timeout: 0,
            url: Url::default(),
            topic: String::new(),
        },
        hostname: String::new(),
        client: None,
    };

    assert!(sink.asynchronous());
}
