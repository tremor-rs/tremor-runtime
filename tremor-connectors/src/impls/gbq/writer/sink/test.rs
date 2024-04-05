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
use crate::{
    harness::Harness,
    impls::gbq,
    reconnect::ConnectionLostNotifier,
    utils::{
        google::{tests::gouth_token, TokenSrc},
        quiescence::QuiescenceBeacon,
    },
};
use anyhow::Result;
use bytes::Bytes;
use futures::future::Ready;
use googapis::google::{
    cloud::bigquery::storage::v1::{
        append_rows_response, table_field_schema::Mode, AppendRowsResponse, TableSchema,
    },
    rpc::Status,
};
use http::{HeaderMap, HeaderValue};
use prost::Message;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::task::Poll;
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tremor_common::ids::SinkId;
use value_trait::StaticNode;

struct HardcodedChannelFactory {
    channel: Channel,
}

#[async_trait::async_trait]
impl ChannelFactory<Channel> for HardcodedChannelFactory {
    async fn make_channel(&self, _connect_timeout: Duration) -> Result<Channel> {
        Ok(self.channel.clone())
    }
}

#[derive(Debug)]
enum MockServiceError {}

impl Display for MockServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockServiceError")
    }
}

impl std::error::Error for MockServiceError {}

struct MockChannelFactory {
    responses: Arc<RwLock<VecDeque<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl ChannelFactory<MockService> for MockChannelFactory {
    async fn make_channel(&self, _connect_timeout: Duration) -> Result<MockService> {
        Ok(MockService {
            responses: self.responses.clone(),
        })
    }
}

#[derive(Clone)]
struct MockService {
    responses: Arc<RwLock<VecDeque<Vec<u8>>>>,
}

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
        let buffer = self.responses.write().unwrap().pop_front().unwrap();

        let (mut tx, body) = tonic::transport::Body::channel();
        let jh = tokio::task::spawn(async move {
            let len: [u8; 4] = (buffer.len() as u32).to_be_bytes();

            let mut response_buffer = vec![0u8];
            response_buffer.append(&mut len.to_vec());
            response_buffer.append(&mut buffer.clone());

            tx.send_data(Bytes::from(response_buffer)).await.unwrap();

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

#[test]
fn skips_unknown_field_types() {
    let (rx, _tx) = crate::channel::bounded(1024);
    let alias = alias::Connector::new("flow", "connector");
    let result = map_field(
        "name",
        &vec![TableFieldSchema {
            name: "something".to_string(),
            r#type: -1,
            mode: Mode::Required.into(),
            fields: vec![],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &SinkContext::new(
            SinkId::default(),
            alias.clone(),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, rx),
        ),
    );

    assert_eq!(result.0.field.len(), 0);
    assert_eq!(result.1.len(), 0);
}

#[test]
fn skips_fields_of_unspecified_type() {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let result = map_field(
        "name",
        &vec![TableFieldSchema {
            name: "something".to_string(),
            r#type: TableType::Unspecified.into(),
            mode: Mode::Required.into(),
            fields: vec![],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &SinkContext::new(
            SinkId::default(),
            alias.clone(),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, rx),
        ),
    );

    assert_eq!(result.0.field.len(), 0);
    assert_eq!(result.1.len(), 0);
}

#[test]
fn can_map_simple_field() {
    let data = vec![
        (TableType::Int64, field_descriptor_proto::Type::Int64),
        (TableType::Double, field_descriptor_proto::Type::Double),
        (TableType::Bool, field_descriptor_proto::Type::Bool),
        (TableType::Bytes, field_descriptor_proto::Type::Bytes),
        (TableType::Timestamp, field_descriptor_proto::Type::String),
    ];

    for item in data {
        let (rx, _tx) = bounded(1024);
        let alias = alias::Connector::new("flow", "connector");

        let result = map_field(
            "name",
            &vec![TableFieldSchema {
                name: "something".to_string(),
                r#type: item.0.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &SinkContext::new(
                SinkId::default(),
                alias.clone(),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(&alias, rx),
            ),
        );

        assert_eq!(result.1.len(), 1);
        assert_eq!(result.1["something"].table_type, item.0);
        assert_eq!(result.0.field[0].r#type, Some(item.1.into()));
    }
}

#[test]
fn can_map_a_struct() {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let result = map_field(
        "name",
        &vec![TableFieldSchema {
            name: "something".to_string(),
            r#type: TableType::Struct.into(),
            mode: Mode::Required.into(),
            fields: vec![TableFieldSchema {
                name: "subfield_a".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &SinkContext::new(
            SinkId::default(),
            alias.clone(),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(&alias, rx),
        ),
    );

    assert_eq!(result.1.len(), 1);
    assert_eq!(result.1["something"].table_type, TableType::Struct);
    assert_eq!(
        result.0.field[0].r#type,
        Some(field_descriptor_proto::Type::Message.into())
    );
    assert_eq!(result.1["something"].subfields.len(), 1);
    assert_eq!(
        result.1["something"].subfields["subfield_a"].table_type,
        TableType::Int64
    );
}

#[test]
fn encode_fails_on_type_mismatch() {
    let data = [
        (
            Value::String("asdf".into()),
            Field {
                table_type: TableType::Int64,
                tag: 1,
                subfields: HashMap::default(),
            },
        ),
        (
            Value::Static(StaticNode::F64(1.243)),
            Field {
                table_type: TableType::String,
                tag: 2,
                subfields: HashMap::default(),
            },
        ),
    ];

    for (value, field) in data {
        let mut result_data = vec![];

        let result = encode_field(&value, &field, &mut result_data);

        assert!(result.is_err());
    }
}

#[test]
pub fn can_encode_stringy_types() {
    // NOTE: This test always passes the string "I" as the value to encode, this is not correct for some of the types (e.g. datetime),
    // but we still allow it, leaving the validation to BigQuery
    let data = [
        TableType::String,
        TableType::Date,
        TableType::Time,
        TableType::Datetime,
        TableType::Geography,
        TableType::Numeric,
        TableType::Bignumeric,
        TableType::Timestamp,
    ];

    for item in data {
        let mut result = vec![];
        assert!(
            encode_field(
                &Value::String("I".into()),
                &Field {
                    table_type: item,
                    tag: 123,
                    subfields: HashMap::default()
                },
                &mut result
            )
            .is_ok(),
            "TableType: {item:?} did not encode correctly",
        );

        assert_eq!([218u8, 7u8, 1u8, 73u8], result[..]);
    }
}

#[test]
pub fn can_encode_a_struct() {
    let mut input = Value::object();
    input.try_insert("a", 1);
    input.try_insert("b", 1024);

    let mut subfields = HashMap::new();
    subfields.insert(
        "a".into(),
        Field {
            table_type: TableType::Int64,
            tag: 1,
            subfields: HashMap::default(),
        },
    );
    subfields.insert(
        "b".into(),
        Field {
            table_type: TableType::Int64,
            tag: 2,
            subfields: HashMap::default(),
        },
    );

    let field = Field {
        table_type: TableType::Struct,
        tag: 1024,
        subfields,
    };

    let mut result = Vec::new();
    assert!(encode_field(&input, &field, &mut result).is_ok());

    assert_eq!([130u8, 64u8, 5u8, 8u8, 1u8, 16u8, 128u8, 8u8], result[..]);
}

#[test]
pub fn can_encode_a_double() {
    let value = Value::Static(StaticNode::F64(1.2345));
    let field = Field {
        table_type: TableType::Double,
        tag: 2,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    assert_eq!(
        [17u8, 141u8, 151u8, 110u8, 18u8, 131u8, 192u8, 243u8, 63u8],
        result[..]
    );
}

#[test]
pub fn can_encode_boolean() {
    let value = Value::Static(StaticNode::Bool(false));
    let field = Field {
        table_type: TableType::Bool,
        tag: 43,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    assert_eq!([216u8, 2u8, 0u8], result[..]);
}

#[test]
pub fn can_encode_bytes() {
    let value = Value::Bytes(vec![0x1u8, 0x2u8, 0x3u8].into());
    let field = Field {
        table_type: TableType::Bytes,
        tag: 1,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    assert_eq!([10u8, 3u8, 1u8, 2u8, 3u8], result[..]);
}

#[test]
pub fn can_encode_json() {
    let value = Value::object();
    let field = Field {
        table_type: TableType::Json,
        tag: 1,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    // json is currently not supported, so we expect the field to be skipped
    assert_eq!([] as [u8; 0], result[..]);
}

#[test]
pub fn can_encode_interval() {
    let value = Value::String("".into());
    let field = Field {
        table_type: TableType::Interval,
        tag: 1,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    // interval is currently not supported, so we expect the field to be skipped
    assert_eq!([] as [u8; 0], result[..]);
}

#[test]
pub fn can_skips_unspecified() {
    let value = Value::String("".into());
    let field = Field {
        table_type: TableType::Unspecified,
        tag: 1,
        subfields: HashMap::default(),
    };

    let mut result = Vec::new();
    assert!(encode_field(&value, &field, &mut result).is_ok());

    // Fields should never have the "Unspecified" type, if that happens best we can do is to log a warning and ignore them
    assert_eq!([] as [u8; 0], result[..]);
}

#[test]
pub fn mapping_generates_a_correct_descriptor() {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![
            TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
            TableFieldSchema {
                name: "b".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
        ],
        &ctx,
    );

    let descriptor = mapping.descriptor();
    assert_eq!(2, descriptor.field.len());
    assert_eq!(
        descriptor.field[0].r#type,
        Some(field_descriptor_proto::Type::Int64 as i32),
    );
    assert_eq!(
        descriptor.field[1].r#type,
        Some(field_descriptor_proto::Type::Int64 as i32),
    );
}

#[test]
pub fn can_map_json_to_protobuf() -> anyhow::Result<()> {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![
            TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
            TableFieldSchema {
                name: "b".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
        ],
        &ctx,
    );
    let mut fields = Value::object();
    fields.try_insert("a", 12);
    fields.try_insert("b", 21);
    let result = mapping.map(&fields)?;

    assert_eq!([8u8, 12u8, 16u8, 21u8], result[..]);
    Ok(())
}

#[test]
fn map_field_ignores_fields_that_are_not_in_definition() -> anyhow::Result<()> {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![
            TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
            TableFieldSchema {
                name: "b".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            },
        ],
        &ctx,
    );
    let mut fields = Value::object();
    fields.try_insert("a", 12);
    fields.try_insert("b", 21);
    fields.try_insert("c", 33);
    let result = mapping.map(&fields)?;

    assert_eq!([8u8, 12u8, 16u8, 21u8], result[..]);
    Ok(())
}

#[test]
fn map_field_ignores_struct_fields_that_are_not_in_definition() -> anyhow::Result<()> {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![TableFieldSchema {
            name: "a".to_string(),
            r#type: TableType::Struct.into(),
            mode: Mode::Required.into(),
            fields: vec![TableFieldSchema {
                name: "x".to_string(),
                r#type: TableType::Int64.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &ctx,
    );
    let mut inner_fields = Value::object();
    inner_fields.try_insert("x", 10);
    inner_fields.try_insert("y", 10);
    let mut fields = Value::object();
    fields.try_insert("a", inner_fields);
    let result = mapping.map(&fields)?;

    assert_eq!([10u8, 2u8, 8u8, 10u8], result[..]);
    Ok(())
}

#[test]
fn fails_on_bytes_type_mismatch() {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![TableFieldSchema {
            name: "a".to_string(),
            r#type: TableType::Bytes.into(),
            mode: Mode::Required.into(),
            fields: vec![],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &ctx,
    );
    let mut fields = Value::object();
    fields.try_insert("a", 12);
    let result = mapping.map(&fields);

    matches!(
        result,
        Err(TryTypeError {
            got: ValueType::I64,
            expected: ValueType::Custom("bytes")
        })
    );
}

#[test]
fn fails_if_the_event_is_not_an_object() {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, rx),
    );
    let mapping = JsonToProtobufMapping::new(
        &vec![TableFieldSchema {
            name: "a".to_string(),
            r#type: TableType::Bytes.into(),
            mode: Mode::Required.into(),
            fields: vec![],
            description: String::new(),
            max_length: 0,
            precision: 0,
            scale: 0,
        }],
        &ctx,
    );
    let result = mapping.map(&Value::Static(StaticNode::I64(123)));

    matches!(
        result,
        Err(TryTypeError {
            got: ValueType::I64,
            expected: ValueType::Object
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_fails_if_config_is_missing() -> anyhow::Result<()> {
    let config = literal!({
        "config": {}
    });

    let result = Harness::new("test", &gbq::writer::Builder::default(), &config).await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn on_event_fails_if_client_is_not_conected() -> anyhow::Result<()> {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");
    // note we need to keep this around until the end of the test so we can't consume it
    let token_file = gouth_token().await?;

    let config = Config::new(&literal!({
        "token": {"file": token_file.cert_file()},
        "table_id": "doesnotmatter",
        "connect_timeout": 1_000_000,
        "request_timeout": 1_000_000
    }))?;

    let mut sink = GbqSink::<_, _>::new(config, Box::new(TonicChannelFactory));

    let result = sink
        .on_event(
            "",
            Event::signal_tick(),
            &SinkContext::new(
                SinkId::default(),
                alias.clone(),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(&alias, rx),
            ),
            &mut EventSerializer::new(
                None,
                CodecReq::Structured,
                vec![],
                &ConnectorType::from(""),
                &alias.clone(),
            )?,
            0,
        )
        .await;

    assert!(result.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn on_event_fails_if_write_stream_is_not_conected() -> anyhow::Result<()> {
    let (rx, _tx) = bounded(1024);
    let alias = alias::Connector::new("flow", "connector");
    // note we need to keep this around until the end of the test so we can't consume it
    let token_file = gouth_token().await?;

    let config = Config::new(&literal!({
        "token": {"file": token_file.cert_file()},
        "table_id": "doesnotmatter",
        "connect_timeout": 1_000_000,
        "request_timeout": 1_000_000
    }))?;

    let mut sink = GbqSink::<_, _>::new(
        config,
        Box::new(HardcodedChannelFactory {
            channel: Channel::from_static("http://example.com").connect_lazy(),
        }),
    );

    let result = sink
        .on_event(
            "",
            Event::signal_tick(),
            &SinkContext::new(
                SinkId::default(),
                alias.clone(),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(&alias, rx),
            ),
            &mut EventSerializer::new(
                None,
                CodecReq::Structured,
                vec![],
                &ConnectorType::from(""),
                &alias.clone(),
            )?,
            0,
        )
        .await;

    assert!(result.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub async fn fails_on_error_response() -> anyhow::Result<()> {
    let mut buffer_write_stream = vec![];
    let mut buffer_append_rows_response = vec![];

    let alias = alias::Connector::new("flow", "connector");
    WriteStream {
        name: "test".to_string(),
        r#type: i32::from(write_stream::Type::Committed),
        create_time: None,
        commit_time: None,
        table_schema: Some(TableSchema {
            fields: vec![TableFieldSchema {
                name: "newfield".to_string(),
                r#type: i32::from(table_field_schema::Type::String),
                mode: i32::from(Mode::Required),
                fields: vec![],
                description: "test".to_string(),
                max_length: 10,
                precision: 0,
                scale: 0,
            }],
        }),
    }
    .encode(&mut buffer_write_stream)
    .expect("encode failed");

    AppendRowsResponse {
        updated_schema: Some(TableSchema {
            fields: vec![TableFieldSchema {
                name: "newfield".to_string(),
                r#type: i32::from(table_field_schema::Type::String),
                mode: i32::from(Mode::Required),
                fields: vec![],
                description: "test".to_string(),
                max_length: 10,
                precision: 0,
                scale: 0,
            }],
        }),
        response: Some(append_rows_response::Response::Error(Status {
            code: 1024,
            message: "test failure".to_string(),
            details: vec![],
        })),
    }
    .encode(&mut buffer_append_rows_response)
    .expect("encode failed");

    let responses = Arc::new(RwLock::new(VecDeque::from([
        buffer_write_stream,
        buffer_append_rows_response,
    ])));
    let mock = gouth_token().await?;

    let mut sink = GbqSink::<_, _>::new(
        Config {
            token: TokenSrc::File(mock.cert_file()),
            table_id: String::new(),
            connect_timeout: 1_000_000_000,
            request_timeout: 1_000_000_000,
            request_size_limit: 10 * 1024 * 1024,
        },
        Box::new(MockChannelFactory {
            responses: responses.clone(),
        }),
    );

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, crate::channel::bounded(1024).0),
    );

    sink.connect(&ctx, &Attempt::default()).await?;

    let mut event = Event {
        data: EventPayload::from(literal!({
          "newfield": "test"
        })),
        ..Event::default()
    };
    event.transactional = true;

    let result = sink
        .on_event(
            "",
            event,
            &ctx,
            &mut EventSerializer::new(
                None,
                CodecReq::Structured,
                vec![],
                &ConnectorType::from(""),
                &alias.clone(),
            )?,
            0,
        )
        .await?;
    assert_eq!(result.ack, SinkAck::Fail);
    assert_eq!(result.cb, CbAction::None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub async fn splits_large_requests() -> anyhow::Result<()> {
    let mut buffer_write_stream = vec![];
    let mut buffer_append_rows_response = vec![];
    let alias = alias::Connector::new("flow", "connector");
    let mock = gouth_token().await?;

    WriteStream {
        name: "test".to_string(),
        r#type: i32::from(write_stream::Type::Committed),
        create_time: None,
        commit_time: None,
        table_schema: Some(TableSchema {
            fields: vec![TableFieldSchema {
                name: "a".to_string(),
                r#type: table_field_schema::Type::String as i32,
                mode: table_field_schema::Mode::Nullable as i32,
                fields: vec![],
                description: String::new(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
        }),
    }
    .encode(&mut buffer_write_stream)
    .expect("encode failed");

    AppendRowsResponse {
        updated_schema: None,
        response: Some(append_rows_response::Response::AppendResult(AppendResult {
            offset: None,
        })),
    }
    .encode(&mut buffer_append_rows_response)
    .expect("encode failed");

    let responses = Arc::new(RwLock::new(VecDeque::from([
        buffer_write_stream,
        buffer_append_rows_response.clone(),
        buffer_append_rows_response,
    ])));
    let mut sink = GbqSink::<_, _>::new(
        Config {
            token: mock.token_src(),
            table_id: String::new(),
            connect_timeout: 1_000_000_000,
            request_timeout: 1_000_000_000,
            request_size_limit: 16 * 1024,
        },
        Box::new(MockChannelFactory {
            responses: responses.clone(),
        }),
    );

    let ctx = SinkContext::new(
        SinkId::default(),
        alias.clone(),
        ConnectorType::default(),
        QuiescenceBeacon::default(),
        ConnectionLostNotifier::new(&alias, crate::channel::bounded(1024).0),
    );

    sink.connect(&ctx, &Attempt::default()).await?;

    let value = literal!([
        {
            "data": {
                "value": {
                    "a": "a".repeat(15*1024)
                },
                "meta": {}
            }
        },
        {
            "data": {
                "value": {
                    "a": "b".repeat(15*1024)
                },
                "meta": {}
            }
        }
    ]);

    let payload: EventPayload = value.into();

    let result = sink
        .on_event(
            "",
            Event {
                data: payload,
                is_batch: true,
                ..Default::default()
            },
            &ctx,
            &mut EventSerializer::new(
                None,
                CodecReq::Structured,
                vec![],
                &ConnectorType::from(""),
                &alias.clone(),
            )?,
            0,
        )
        .await?;

    assert_eq!(result.ack, SinkAck::Ack);
    assert_eq!(responses.read().expect("failed to get response").len(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub async fn does_not_auto_ack() {
    let mock = gouth_token().await.expect("failed to get token");

    let sink = GbqSink::<_, _>::new(
        Config {
            token: mock.token_src(),
            table_id: String::new(),
            connect_timeout: 1_000_000_000,
            request_timeout: 1_000_000_000,
            request_size_limit: 10 * 1024 * 1024,
        },
        Box::new(MockChannelFactory {
            responses: Arc::new(RwLock::new(VecDeque::new())),
        }),
    );

    assert!(!sink.auto_ack());
}
