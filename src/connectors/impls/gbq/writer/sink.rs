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

use crate::connectors::google::ChannelFactory;
use crate::connectors::{
    google::{AuthInterceptor, TokenProvider},
    impls::gbq::writer::Config,
    prelude::*,
};
use futures::{stream, StreamExt};
use googapis::google::cloud::bigquery::storage::v1::{
    append_rows_request::{self, ProtoData},
    append_rows_response::{AppendResult, Response},
    big_query_write_client::BigQueryWriteClient,
    table_field_schema::{self, Type as TableType},
    write_stream, AppendRowsRequest, CreateWriteStreamRequest, ProtoRows, ProtoSchema,
    TableFieldSchema, WriteStream,
};
use prost::encoding::WireType;
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use std::collections::hash_map::Entry;
use std::marker::PhantomData;
use std::{collections::HashMap, time::Duration};
use tokio::time::timeout;
use tonic::{
    codegen::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig},
};

pub(crate) struct TonicChannelFactory;

#[async_trait::async_trait]
impl ChannelFactory<Channel> for TonicChannelFactory {
    async fn make_channel(&self, connect_timeout: Duration) -> Result<Channel> {
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("bigquerystorage.googleapis.com");

        Ok(
            Channel::from_static("https://bigquerystorage.googleapis.com")
                .connect_timeout(connect_timeout)
                .tls_config(tls_config)?
                .connect()
                .await?,
        )
    }
}

struct ConnectedWriteStream {
    name: String,
    mapping: JsonToProtobufMapping,
}

pub(crate) struct GbqSink<
    T: TokenProvider,
    TChannel: GbqChannel<TChannelError>,
    TChannelError: GbqChannelError,
> {
    client: Option<BigQueryWriteClient<InterceptedService<TChannel, AuthInterceptor<T>>>>,
    write_streams: HashMap<String, ConnectedWriteStream>,
    config: Config,
    channel_factory: Box<dyn ChannelFactory<TChannel> + Send + Sync>,
    _error_phantom: PhantomData<TChannelError>,
}

struct Field {
    table_type: TableType,
    tag: u32,
    // mode: Mode,

    // ignored if the table_type is not struct
    subfields: HashMap<String, Field>,
}

struct JsonToProtobufMapping {
    fields: HashMap<String, Field>,
    descriptor: DescriptorProto,
}

fn map_field(
    schema_name: &str,
    raw_fields: &Vec<TableFieldSchema>,
    ctx: &SinkContext,
) -> (DescriptorProto, HashMap<String, Field>) {
    // The capacity for nested_types isn't known here, as it depends on the number of fields that have the struct type
    let mut nested_types = vec![];
    let mut proto_fields = Vec::with_capacity(raw_fields.len());
    let mut fields = HashMap::with_capacity(raw_fields.len());
    let mut tag: u16 = 1;

    for raw_field in raw_fields {
        let mut type_name = None;
        let mut subfields = HashMap::with_capacity(raw_field.fields.len());

        let table_type =
            if let Some(table_type) = table_field_schema::Type::from_i32(raw_field.r#type) {
                table_type
            } else {
                warn!("{ctx} Found a field of unknown type: {}", raw_field.name);

                continue;
            };

        let grpc_type = match table_type {
            TableType::Int64 => field_descriptor_proto::Type::Int64,
            TableType::Double => field_descriptor_proto::Type::Double,
            TableType::Bool => field_descriptor_proto::Type::Bool,
            TableType::Bytes => field_descriptor_proto::Type::Bytes,


            TableType::String
            // YYYY-[M]M-[D]D
            | TableType::Date
            // [H]H:[M]M:[S]S[.DDDDDD|.F]
            | TableType::Time
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]
            | TableType::Datetime
            // The GEOGRAPHY type is based on the OGC Simple Features specification (SFS)
            | TableType::Geography
            // String, because it has decimal precision, f32/f64 would lose precision
            | TableType::Numeric
            | TableType::Bignumeric
            // [sign]Y-M [sign]D [sign]H:M:S[.F]
            | TableType::Interval
            | TableType::Json
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]][time zone]
            | TableType::Timestamp => field_descriptor_proto::Type::String,
            TableType::Struct => {
                let type_name_for_field = format!("struct_{}", raw_field.name);
                let mapped = map_field(&type_name_for_field, &raw_field.fields, ctx);
                nested_types.push(mapped.0);
                subfields = mapped.1;

                type_name = Some(type_name_for_field);
                field_descriptor_proto::Type::Message
            }

            TableType::Unspecified => {
                warn!("{} Found a field of unspecified type: {}", ctx, raw_field.name);
                continue;
            }
        };

        proto_fields.push(FieldDescriptorProto {
            name: Some(raw_field.name.to_string()),
            number: Some(i32::from(tag)),
            label: None,
            r#type: Some(i32::from(grpc_type)),
            type_name,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });

        fields.insert(
            raw_field.name.to_string(),
            Field {
                // mode: Mode::from(raw_field.mode),
                table_type,
                tag: u32::from(tag),
                subfields,
            },
        );

        tag += 1;
    }

    (
        DescriptorProto {
            name: Some(schema_name.to_string()),
            field: proto_fields,
            extension: vec![],
            nested_type: nested_types,
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        },
        fields,
    )
}

fn encode_field(val: &Value, field: &Field, result: &mut Vec<u8>) -> Result<()> {
    let tag = field.tag;

    match field.table_type {
        TableType::Double => prost::encoding::double::encode(
            tag,
            &val.as_f64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("f64", val.value_type()))?,
            result,
        ),
        TableType::Int64 => prost::encoding::int64::encode(
            tag,
            &val.as_i64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("i64", val.value_type()))?,
            result,
        ),
        TableType::Bool => prost::encoding::bool::encode(
            tag,
            &val.as_bool()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("bool", val.value_type()))?,
            result,
        ),
        TableType::String
        | TableType::Date
        | TableType::Time
        | TableType::Datetime
        | TableType::Timestamp
        // String, because it has decimal precision, f32/f64 would lose precision
        | TableType::Numeric
        | TableType::Bignumeric
        | TableType::Geography => {
            prost::encoding::string::encode(
                tag,
                &val.as_str()
                    .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("string", val.value_type()))?
                    .to_string(),
                result,
            );
        }
        TableType::Struct => {
            let mut struct_buf: Vec<u8> = vec![];
            for (k, v) in val
                .as_object()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("object", val.value_type()))?
            {
                let subfield_description = field.subfields.get(&k.to_string());

                if let Some(subfield_description) = subfield_description {
                    encode_field(v, subfield_description, &mut struct_buf)?;
                } else {
                    warn!(
                        "Passed field {} as struct field, not present in definition",
                        k
                    );
                }
            }
            prost::encoding::encode_key(tag, WireType::LengthDelimited, result);
            prost::encoding::encode_varint(struct_buf.len() as u64, result);
            result.append(&mut struct_buf);
        }
        TableType::Bytes => {
            prost::encoding::bytes::encode(
                tag,
                &Vec::from(
                    val.as_bytes().ok_or_else(|| {
                        ErrorKind::BigQueryTypeMismatch("bytes", val.value_type())
                    })?,
                ),
                result,
            );
        }
        TableType::Json => {
            warn!("Found a field of type JSON, this is not supported, ignoring.");
        }
        TableType::Interval => {
            warn!("Found a field of type Interval, this is not supported, ignoring.");
        }

        TableType::Unspecified => {
            warn!("Found a field of unspecified type - ignoring.");
        }
    }

    Ok(())
}

impl JsonToProtobufMapping {
    pub fn new(vec: &Vec<TableFieldSchema>, ctx: &SinkContext) -> Self {
        let descriptor = map_field("table", vec, ctx);

        Self {
            descriptor: descriptor.0,
            fields: descriptor.1,
        }
    }

    pub fn map(&self, value: &Value) -> Result<Vec<u8>> {
        if let Some(obj) = value.as_object() {
            let mut result = Vec::with_capacity(obj.len());

            for (key, val) in obj {
                let k: &str = key;
                if let Some(field) = self.fields.get(k) {
                    encode_field(val, field, &mut result)?;
                }
            }
            return Ok(result);
        }
        Err(ErrorKind::BigQueryTypeMismatch("object", value.value_type()).into())
    }

    pub fn descriptor(&self) -> &DescriptorProto {
        &self.descriptor
    }
}
impl<T: TokenProvider, TChannel: GbqChannel<TChannelError>, TChannelError: GbqChannelError>
    GbqSink<T, TChannel, TChannelError>
{
    pub fn new(
        config: Config,
        channel_factory: Box<dyn ChannelFactory<TChannel> + Send + Sync>,
    ) -> Self {
        Self {
            client: None,
            write_streams: HashMap::new(),
            config,
            channel_factory,
            _error_phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<
        T: TokenProvider + 'static,
        TChannel: GbqChannel<TChannelError> + 'static,
        TChannelError: GbqChannelError,
    > Sink for GbqSink<T, TChannel, TChannelError>
where
    TChannel::Future: Send,
{
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let request_size_limit = self.config.request_size_limit;

        let request_data = self
            .event_to_requests(event, ctx, request_size_limit)
            .await?;

        if request_data.len() > 1 {
            warn!("{ctx} The batch is too large to be sent in a single request, splitting it into {} requests. Consider lowering the batch size.", request_data.len());
        }

        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "BigQuery",
            "The client is not connected",
        ))?;
        for request in request_data {
            let req_timeout = Duration::from_nanos(self.config.request_timeout);
            let append_response =
                timeout(req_timeout, client.append_rows(stream::iter(vec![request]))).await;

            let append_response = if let Ok(append_response) = append_response {
                append_response
            } else {
                // timeout sending append rows request
                error!(
                    "{ctx} GBQ request timed out after {}ms",
                    req_timeout.as_millis()
                );
                ctx.notifier().connection_lost().await?;

                return Ok(SinkReply::FAIL);
            };

            if let Ok(x) = timeout(req_timeout, append_response?.into_inner().next()).await {
                match x {
                    Some(Ok(res)) => {
                        if let Some(updated_schema) = res.updated_schema.as_ref() {
                            let fields = updated_schema
                                .fields
                                .iter()
                                .map(|f| {
                                    format!(
                                        "{}: {:?}",
                                        f.name,
                                        TableType::from_i32(f.r#type).unwrap_or_default()
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("\n");

                            info!("{ctx} GBQ Schema was updated: {}", fields);
                        }
                        if let Some(res) = res.response {
                            match res {
                                Response::AppendResult(AppendResult { .. }) => {}
                                Response::Error(e) => {
                                    error!("{ctx} GBQ Error: {} {}", e.code, e.message);
                                    return Ok(SinkReply::FAIL);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("{ctx} GBQ Error: {}", e);
                        return Ok(SinkReply::FAIL);
                    }
                    None => return Ok(SinkReply::NONE),
                }
            } else {
                // timeout receiving response
                error!(
                    "{ctx} Receiving GBQ response timeout after {}ms",
                    req_timeout.as_millis()
                );
                ctx.notifier().connection_lost().await?;

                return Ok(SinkReply::FAIL);
            }
        }

        Ok(SinkReply::ACK)
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        info!("{ctx} Connecting to BigQuery");

        let channel = self
            .channel_factory
            .make_channel(Duration::from_nanos(self.config.connect_timeout))
            .await?;

        let client = BigQueryWriteClient::with_interceptor(
            channel,
            AuthInterceptor {
                token_provider: T::from(self.config.token.clone()),
            },
        );
        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

pub trait GbqChannel<TChannelError>:
    tonic::codegen::Service<
        http::Request<tonic::body::BoxBody>,
        Response = http::Response<tonic::transport::Body>,
        Error = TChannelError,
    > + Send
    + Clone
where
    TChannelError: GbqChannelError,
{
}

pub trait GbqChannelError:
    Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync
{
}

impl<T> GbqChannelError for T where
    T: Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync
{
}
impl<T, TChannelError> GbqChannel<TChannelError> for T
where
    T: tonic::codegen::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::transport::Body>,
            Error = TChannelError,
        > + Send
        + Clone,
    TChannelError: GbqChannelError,
{
}

impl<T, TChannelError, TChannel> GbqSink<T, TChannel, TChannelError>
where
    T: TokenProvider + 'static,
    TChannel: GbqChannel<TChannelError> + 'static,
    TChannel::Future: Send,
    TChannelError: GbqChannelError,
{
    async fn event_to_requests(
        &mut self,
        event: Event,
        ctx: &SinkContext,
        request_size_limit: usize,
    ) -> Result<Vec<AppendRowsRequest>> {
        let mut request_data: Vec<AppendRowsRequest> = Vec::new();
        let mut requests: HashMap<_, Vec<_>> = HashMap::new();

        for (data, meta) in event.value_meta_iter() {
            requests
                .entry(
                    ctx.extract_meta(meta)
                        .get("table_id")
                        .as_str()
                        .map(String::from),
                )
                .or_default()
                .push(data);
        }

        for (tid, data) in requests {
            let tid = tid.map_or_else(|| self.config.table_id.clone(), String::from);
            let write_stream = self.get_or_create_write_stream(tid, ctx).await?;

            let data_len = data.len();

            let mut serialized_rows = Vec::with_capacity(data_len);
            let mut size = 0;
            for serialized in data.into_iter().map(|d| write_stream.mapping.map(d)) {
                let serialized = serialized?;
                if size + serialized.len() > request_size_limit {
                    let last_len = serialized_rows.len();

                    request_data.push(AppendRowsRequest {
                        write_stream: write_stream.name.clone(),
                        offset: None,
                        rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
                            writer_schema: Some(ProtoSchema {
                                proto_descriptor: Some(write_stream.mapping.descriptor().clone()),
                            }),
                            rows: Some(ProtoRows { serialized_rows }),
                        })),
                        trace_id: String::new(),
                    });
                    size = 0;
                    serialized_rows = Vec::with_capacity(data_len - last_len);
                }
                size += serialized.len();
                serialized_rows.push(serialized);
            }

            if !serialized_rows.is_empty() {
                request_data.push(AppendRowsRequest {
                    write_stream: write_stream.name.clone(),
                    offset: None,
                    rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
                        writer_schema: Some(ProtoSchema {
                            proto_descriptor: Some(write_stream.mapping.descriptor().clone()),
                        }),
                        rows: Some(ProtoRows { serialized_rows }),
                    })),
                    trace_id: String::new(),
                });
            }
        }

        Ok(request_data)
    }
}

impl<
        T: TokenProvider + 'static,
        TChannel: GbqChannel<TChannelError> + 'static,
        TChannelError: GbqChannelError,
    > GbqSink<T, TChannel, TChannelError>
where
    TChannel::Future: Send,
{
    async fn get_or_create_write_stream(
        &mut self,
        table_id: String,
        ctx: &SinkContext,
    ) -> Result<&ConnectedWriteStream> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "BigQuery",
            "The client is not connected",
        ))?;

        match self.write_streams.entry(table_id.clone()) {
            Entry::Occupied(entry) => {
                // NOTE: `into_mut` is needed here, even though we just need a non-mutable reference
                //  This is because `get` returns reference which's lifetime is bound to the entry,
                //  while the reference returned by `into_mut` is bound to the map
                Ok(entry.into_mut())
            }
            Entry::Vacant(entry) => {
                let stream = client
                    .create_write_stream(CreateWriteStreamRequest {
                        parent: table_id.clone(),
                        write_stream: Some(WriteStream {
                            // The stream name here will be ignored and a generated value will be set in the response
                            name: String::new(),
                            r#type: i32::from(write_stream::Type::Committed),
                            create_time: None,
                            commit_time: None,
                            table_schema: None,
                        }),
                    })
                    .await?
                    .into_inner();

                let mapping = JsonToProtobufMapping::new(
                    &stream
                        .table_schema
                        .as_ref()
                        .ok_or_else(|| ErrorKind::GbqSchemaNotProvided(table_id))?
                        .clone()
                        .fields,
                    ctx,
                );

                Ok(entry.insert(ConnectedWriteStream {
                    name: stream.name,
                    mapping,
                }))
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "gcp-integration")]
mod test {
    use super::*;
    use crate::connectors::{
        google::{tests::TestTokenProvider, TokenSrc},
        impls::gbq,
        reconnect::ConnectionLostNotifier,
        tests::ConnectorHarness,
        utils::quiescence::QuiescenceBeacon,
    };
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
                alias::Connector::new("flow", "connector"),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(rx),
            ),
        );

        assert_eq!(result.0.field.len(), 0);
        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn skips_fields_of_unspecified_type() {
        let (rx, _tx) = bounded(1024);

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
                alias::Connector::new("flow", "connector"),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(rx),
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
                    alias::Connector::new("flow", "connector"),
                    ConnectorType::default(),
                    QuiescenceBeacon::default(),
                    ConnectionLostNotifier::new(rx),
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
                alias::Connector::new("flow", "connector"),
                ConnectorType::default(),
                QuiescenceBeacon::default(),
                ConnectionLostNotifier::new(rx),
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

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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
    pub fn can_map_json_to_protobuf() -> Result<()> {
        let (rx, _tx) = bounded(1024);

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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
    fn map_field_ignores_fields_that_are_not_in_definition() -> Result<()> {
        let (rx, _tx) = bounded(1024);

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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
    fn map_field_ignores_struct_fields_that_are_not_in_definition() -> Result<()> {
        let (rx, _tx) = bounded(1024);

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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

        if let Err(Error(ErrorKind::BigQueryTypeMismatch("bytes", x), _)) = result {
            assert_eq!(x, ValueType::I64);
        } else {
            panic!("Bytes conversion did not fail on type mismatch");
        }
    }

    #[test]
    fn fails_if_the_event_is_not_an_object() {
        let (rx, _tx) = bounded(1024);

        let ctx = SinkContext::new(
            SinkId::default(),
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(rx),
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

        if let Err(Error(ErrorKind::BigQueryTypeMismatch("object", x), _)) = result {
            assert_eq!(x, ValueType::I64);
        } else {
            panic!("Mapping did not fail on non-object event");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_fails_if_config_is_missing() -> Result<()> {
        let config = literal!({
            "config": {}
        });

        let result =
            ConnectorHarness::new(function_name!(), &gbq::writer::Builder::default(), &config)
                .await;

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_fails_if_client_is_not_conected() -> Result<()> {
        let (rx, _tx) = bounded(1024);
        let config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
            "table_id": "doesnotmatter",
            "connect_timeout": 1_000_000,
            "request_timeout": 1_000_000
        }))?;

        let mut sink =
            GbqSink::<TestTokenProvider, _, _>::new(config, Box::new(TonicChannelFactory));

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext::new(
                    SinkId::default(),
                    alias::Connector::new("flow", "connector"),
                    ConnectorType::default(),
                    QuiescenceBeacon::default(),
                    ConnectionLostNotifier::new(rx),
                ),
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &alias::Connector::new("flow", "connector"),
                )?,
                0,
            )
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_fails_if_write_stream_is_not_conected() -> Result<()> {
        let (rx, _tx) = bounded(1024);
        let config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
            "table_id": "doesnotmatter",
            "connect_timeout": 1_000_000,
            "request_timeout": 1_000_000
        }))?;

        let mut sink = GbqSink::<TestTokenProvider, _, _>::new(
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
                    alias::Connector::new("flow", "connector"),
                    ConnectorType::default(),
                    QuiescenceBeacon::default(),
                    ConnectionLostNotifier::new(rx),
                ),
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &alias::Connector::new("flow", "connector"),
                )?,
                0,
            )
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn fails_on_error_response() -> Result<()> {
        let mut buffer_write_stream = vec![];
        let mut buffer_append_rows_response = vec![];
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
        .map_err(|_| "encode failed")?;

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
        .map_err(|_| "encode failed")?;

        let responses = Arc::new(RwLock::new(VecDeque::from([
            buffer_write_stream,
            buffer_append_rows_response,
        ])));
        let mut sink = GbqSink::<TestTokenProvider, _, _>::new(
            Config {
                token: TokenSrc::dummy(),
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
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(crate::channel::bounded(1024).0),
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
                    &alias::Connector::new("flow", "connector"),
                )?,
                0,
            )
            .await?;
        assert_eq!(result.ack, SinkAck::Fail);
        assert_eq!(result.cb, CbAction::None);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn splits_large_requests() -> Result<()> {
        let mut buffer_write_stream = vec![];
        let mut buffer_append_rows_response = vec![];
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
        .map_err(|_| "encode failed")?;

        AppendRowsResponse {
            updated_schema: None,
            response: Some(append_rows_response::Response::AppendResult(AppendResult {
                offset: None,
            })),
        }
        .encode(&mut buffer_append_rows_response)
        .map_err(|_| "encode failed")?;

        let responses = Arc::new(RwLock::new(VecDeque::from([
            buffer_write_stream,
            buffer_append_rows_response.clone(),
            buffer_append_rows_response,
        ])));
        let mut sink = GbqSink::<TestTokenProvider, _, _>::new(
            Config {
                token: TokenSrc::dummy(),
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
            alias::Connector::new("flow", "connector"),
            ConnectorType::default(),
            QuiescenceBeacon::default(),
            ConnectionLostNotifier::new(crate::channel::bounded(1024).0),
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
                    &alias::Connector::new("flow", "connector"),
                )?,
                0,
            )
            .await?;

        assert_eq!(result.ack, SinkAck::Ack);
        assert_eq!(0, responses.read()?.len());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn does_not_auto_ack() {
        let sink = GbqSink::<TestTokenProvider, _, _>::new(
            Config {
                token: TokenSrc::dummy(),
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
}
