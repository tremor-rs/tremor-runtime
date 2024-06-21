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

#![allow(clippy::cast_possible_wrap)]
use crate::{gcl, utils::tests::gouth_token};

use super::*;
use anyhow::Result;
use futures::future::Ready;
use googapis::google::logging::r#type::LogSeverity;
use googapis::google::logging::v2::WriteLogEntriesResponse;
use http::{HeaderMap, HeaderValue};
use hyper::body::Bytes;
use hyper::body::HttpBody;
use prost::Message;
use std::task::Poll;
use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
};
use tokio::sync::mpsc::{channel, unbounded_channel};
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tremor_common::ids::SinkId;
use tremor_connectors::{
    harness::Harness,
    utils::{quiescence::QuiescenceBeacon, reconnect::ConnectionLostNotifier},
};
use tremor_script::{EventPayload, ValueAndMeta};
use tremor_system::{controlplane::CbAction, event::EventId};
use tremor_value::{literal, structurize};

#[derive(Debug)]
enum MockServiceError {}

impl Display for MockServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockServiceError")
    }
}

impl std::error::Error for MockServiceError {}

struct MockChannelFactory;

#[async_trait::async_trait]
impl ChannelFactory<MockService> for MockChannelFactory {
    async fn make_channel(&self, _connect_timeout: Duration) -> Result<MockService> {
        Ok(MockService {})
    }
}

#[derive(Clone)]
struct MockService {}

impl Service<http::Request<BoxBody>> for MockService {
    type Response = http::Response<tonic::transport::Body>;
    type Error = MockServiceError;
    type Future =
        Ready<std::result::Result<http::Response<tonic::transport::Body>, MockServiceError>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[allow(clippy::unwrap_used, clippy::cast_possible_truncation)] // We don't control the return type here
    fn call(&mut self, _request: http::Request<BoxBody>) -> Self::Future {
        let mut buffer = vec![];

        WriteLogEntriesResponse {}
            .encode_length_delimited(&mut buffer)
            .unwrap();

        let mut response = tonic::transport::Body::from(buffer);
        let (mut tx, body) = tonic::transport::Body::channel();

        let jh = tokio::task::spawn(async move {
            let response = response.data().await.unwrap().unwrap();
            let len: [u8; 4] = (response.len() as u32).to_ne_bytes();
            let len = Bytes::from(len.to_vec());
            tx.send_data(len).await.unwrap();
            tx.send_data(response).await.unwrap();
            let mut trailers = HeaderMap::new();
            trailers.insert(
                "content-type",
                HeaderValue::from_static("application/grpc+proto"),
            );
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            tx.send_trailers(trailers).await.unwrap();
        });
        tokio::task::spawn_blocking(|| jh);

        let response = http::Response::new(body);

        futures::future::ready(Ok(response))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn on_event_can_send_an_event() -> anyhow::Result<()> {
    let (tx, mut rx) = unbounded_channel();
    let (connection_lost_tx, _connection_lost_rx) = channel(10);
    let mock = gouth_token().await?;

    let mut sink = GclSink::<_>::new(
        Config {
            token: mock.token_src(),
            log_name: None,
            resource: None,
            partial_success: false,
            dry_run: false,
            connect_timeout: 0,
            request_timeout: 0,
            default_severity: 0,
            labels: HashMap::default(),
            concurrency: 0,
        },
        tx,
        MockChannelFactory,
    );
    let alias = alias::Connector::new("a", "b");
    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, connection_lost_tx),
    );

    StructuredSink::connect(&mut sink, &ctx, &Attempt::default()).await?;

    let event = Event {
        id: EventId::new(1, 2, 3, 4),
        data: EventPayload::from(ValueAndMeta::from(literal!({
            "message": "test",
            "severity": "INFO",
            "labels": {
                "label1": "value1",
                "label2": "value2"
            }
        }))),
        ..Default::default()
    };
    StructuredSink::on_event(&mut sink, "", event.clone(), &ctx, 0).await?;

    matches!(
        rx.recv().await.expect("no msg"),
        AsyncSinkReply::CB(_, CbAction::Trigger)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn fails_if_the_event_is_not_an_object() -> anyhow::Result<()> {
    let now = tremor_common::time::nanotime();
    let data = &literal!("snot");
    let token_file = gouth_token().await?;

    let config = Config::new(&literal!({
        "token": {"file": token_file.cert_file()},
    }))?;
    let meta = literal!({});
    let meta = meta.get("gcl_writer").or(None);

    let result = value_to_log_entry(now, &config, data, meta);
    matches!(
        result,
        Err(TryTypeError {
            got: value_trait::ValueType::String,
            expected: value_trait::ValueType::Object
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_succeeds_if_config_is_nearly_empty() -> anyhow::Result<()> {
    let token_file = gouth_token().await?;

    let config = literal!({
        "config": {
            "token": {"file": token_file.cert_file()},
        }
    });

    let result = Harness::new("test", &gcl::writer::Builder::default(), &config).await;

    assert!(result.is_ok());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn on_event_fails_if_client_is_not_connected() -> anyhow::Result<()> {
    let (rx, _tx) = channel(1024);
    let (reply_tx, _reply_rx) = unbounded_channel();
    let token_file = gouth_token().await?;

    let config = Config::new(&literal!({
        "token": {"file": token_file.cert_file()},
        "connect_timeout": 1_000_000
    }))?;

    let mut sink = GclSink::<_>::new(config, reply_tx, MockChannelFactory);
    let alias = alias::Connector::new("", "");
    let notifier = ConnectionLostNotifier::new(&alias, rx);
    let result = StructuredSink::on_event(
        &mut sink,
        "",
        Event::signal_tick(),
        &SinkContext::new(
            SinkId::default(),
            alias.clone(),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            notifier,
        ),
        0,
    )
    .await;

    assert!(result.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn log_name_override() -> anyhow::Result<()> {
    let now = tremor_common::time::nanotime();
    let token_file = gouth_token().await?;

    let config: Config = structurize(literal!({
        "token": {"file": token_file.cert_file()},
        "log_name": "snot"
    }))?;
    let data = literal!({"snot": "badger"});
    let meta = literal!({"log_name": "override"});
    let le = value_to_log_entry(now, &config, &data, Some(&meta))?;
    assert_eq!("override", &le.log_name);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn log_severity_override() -> anyhow::Result<()> {
    let token_file = gouth_token().await?;

    let now = tremor_common::time::nanotime();
    let config: Config = structurize(literal!({
        "token": {"file": token_file.cert_file()},
    }))?;
    let data = literal!({"snot": "badger"});
    let meta = literal!({"log_name": "override", "log_severity": LogSeverity::Debug as i32});
    let le = value_to_log_entry(now, &config, &data, Some(&meta))?;
    assert_eq!("override", &le.log_name);
    assert_eq!(LogSeverity::Debug as i32, le.severity);

    Ok(())
}

#[test]
fn prost_value_mappings() -> anyhow::Result<()> {
    use prost_types::value::Kind;
    let v = value_to_prost_value(&literal!(null))?;
    assert_eq!(Some(Kind::NullValue(0)), v.kind);

    let v = value_to_prost_value(&literal!(true))?;
    assert_eq!(Some(Kind::BoolValue(true)), v.kind);

    let v = value_to_prost_value(&literal!(1i64))?;
    assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

    let v = value_to_prost_value(&literal!(1u64))?;
    assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

    let v = value_to_prost_value(&literal!(1f64))?;
    assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

    let v = value_to_prost_value(&literal!("snot"))?;
    assert_eq!(Some(Kind::StringValue("snot".to_string())), v.kind);

    let v = literal!([1u64, 2u64, 3u64]);
    let v = value_to_prost_value(&v)?;
    assert_eq!(
        prost_types::Value {
            kind: Some(Kind::ListValue(prost_types::ListValue {
                values: vec![
                    value_to_prost_value(&literal!(1u64))?,
                    value_to_prost_value(&literal!(2u64))?,
                    value_to_prost_value(&literal!(3u64))?,
                ]
            }))
        },
        v
    );

    let v = literal!({ "snot": "badger"});
    let v = value_to_prost_value(&v)?;
    let mut fields = BTreeMap::new();
    fields.insert(
        "snot".to_string(),
        value_to_prost_value(&literal!("badger"))?,
    );
    assert_eq!(
        prost_types::Value {
            kind: Some(Kind::StructValue(prost_types::Struct { fields }))
        },
        v
    );

    let v = "snot".as_bytes().to_vec().into();
    let v = Value::Bytes(v);
    let v = value_to_prost_value(&v)?;
    assert_eq!(Some(Kind::StringValue("c25vdA==".to_string())), v.kind);

    Ok(())
}

#[test]
fn prost_struct_mapping() {
    let v = literal!({ "snot": "badger"});
    let v = value_to_prost_struct(&v);
    assert!(v.is_ok());

    let v = literal!(null);
    let v = value_to_prost_struct(&v);
    assert!(v.is_err());
}
