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

use crate::errors::Result;
/// Using `GOOGLE_APPLICATION_CREDENTIALS="<path to service token json file>"` this function
/// will authenticate against the google cloud platform using the authentication flow defined
/// in the file. The provided PEM file should be a current non-revoked and complete copy of the
/// required certificate chain for the Google Cloud Platform.
use googapis::google::pubsub::v1::{
    publisher_client::PublisherClient, subscriber_client::SubscriberClient,
};
use googapis::CERTIFICATES;
use gouth::Token;
use tonic::{
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig},
    Request,
};

pub(crate) async fn setup_publisher_client() -> Result<PublisherClient<Channel>> {
    let token = Token::new()?;
    let tls_config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CERTIFICATES))
        .domain_name("pubsub.googleapis.com");

    let channel = Channel::from_static(r#"https://pubsub.googleapis.com/pubsub/v1"#)
        .tls_config(tls_config)?
        .connect()
        .await?;

    let service = PublisherClient::with_interceptor(channel, move |mut req: Request<()>| {
        let token = &*token
            .header_value()
            .expect("Error with token header value in `pubsub_auth`");
        let meta = MetadataValue::from_str(token)
            .expect("Error extracting metadata value from token in `pubsub_auth`");
        req.metadata_mut().insert("authorization", meta);
        Ok(req)
    });
    Ok(service)
}

pub(crate) async fn setup_subscriber_client() -> Result<SubscriberClient<Channel>> {
    let token = Token::new()?;
    let tls_config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CERTIFICATES))
        .domain_name("pubsub.googleapis.com");

    let channel = Channel::from_static(r#"https://pubsub.googleapis.com/pubsub/v1"#)
        .tls_config(tls_config)?
        .connect()
        .await?;

    let service = SubscriberClient::with_interceptor(channel, move |mut req: Request<()>| {
        let token = &*token
            .header_value()
            .expect("Error with token header value in `pubsub_auth`");
        let meta = MetadataValue::from_str(token)
            .expect("Error extracting metadata value from token in `pubsub_auth`");
        req.metadata_mut().insert("authorization", meta);
        Ok(req)
    });
    Ok(service)
}
