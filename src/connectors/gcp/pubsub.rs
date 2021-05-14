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

#![cfg(not(tarpaulin_include))]

use crate::errors::{Error, Result};
use googapis::google::pubsub::v1::{
    publisher_client::PublisherClient, subscriber_client::SubscriberClient,
};
use googapis::google::pubsub::v1::{
    AcknowledgeRequest, PublishRequest, PubsubMessage, PullRequest, PullResponse,
};
use std::collections::HashMap;
use tonic::transport::Channel;

pub(crate) async fn send_message(
    client: &mut PublisherClient<Channel>,
    project_id: &str,
    topic_name: &str,
    data_val: &[u8],
) -> Result<String> {
    let message = PubsubMessage {
        data: data_val.to_vec(),
        attributes: HashMap::new(),
        message_id: "".into(),
        publish_time: None,
        ordering_key: "".to_string(),
    };

    let response = client
        .publish(PublishRequest {
            topic: format!("projects/{}/topics/{}", project_id, topic_name),
            messages: vec![message],
        })
        .await?;
    let p = response.into_inner();
    let res = p
        .message_ids
        .get(0)
        .ok_or_else(|| Error::from("Failed to get message id"))?;
    Ok(res.to_string())
}

pub(crate) async fn receive_message(
    client: &mut SubscriberClient<Channel>,
    project_id: &str,
    subscription_name: &str,
) -> Result<PullResponse> {
    // TODO: Use streaming pull
    #[allow(warnings)]
    // to allow use of deprecated field googapis::google::pubsub::v1::PullRequest::return_immediately
    let response = client
        .pull(PullRequest {
            subscription: format!(
                "projects/{}/subscriptions/{}",
                project_id, subscription_name
            ),
            max_messages: 50,
            return_immediately: false,
        })
        .await?;
    Ok(response.into_inner())
}

pub(crate) async fn acknowledge(
    client: &mut SubscriberClient<Channel>,
    project_id: &str,
    subscription_name: &str,
    ack_ids: Vec<String>,
) -> Result<()> {
    client
        .acknowledge(AcknowledgeRequest {
            subscription: format!(
                "projects/{}/subscriptions/{}",
                project_id, subscription_name
            ),
            ack_ids,
        })
        .await?;
    Ok(())
}
