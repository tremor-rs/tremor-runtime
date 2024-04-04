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

use crate::{
    impls::gbq::writer::Config,
    prelude::*,
    utils::google::{AuthInterceptor, ChannelFactory, TokenProvider},
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
use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    time::Duration,
};
use tokio::time::timeout;
use tonic::{
    codegen::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig},
};

pub(crate) struct TonicChannelFactory;

#[async_trait::async_trait]
impl ChannelFactory<Channel> for TonicChannelFactory {
    async fn make_channel(&self, connect_timeout: Duration) -> anyhow::Result<Channel> {
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

/// GBQ Error
#[derive(Debug, thiserror::Error)]
pub enum GbqError {
    /// Missing Schema
    #[error("Schema for table {0} was not provided")]
    GbqSchemaNotProvided(String),
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

        let Some(table_type) = table_field_schema::Type::from_i32(raw_field.r#type) else {
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

fn encode_field(val: &Value, field: &Field, result: &mut Vec<u8>) -> Result<(), TryTypeError> {
    let tag = field.tag;

    match field.table_type {
        TableType::Double => prost::encoding::double::encode(
            tag,
                &val.try_as_f64()?,
            result,
        ),
        TableType::Int64 => prost::encoding::int64::encode(
            tag,
            &val.try_as_i64()?,
            result,
        ),
        TableType::Bool => prost::encoding::bool::encode(
            tag,
            &val.try_as_bool()?,
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
                &val.try_as_str()?.to_string()
                    ,
                result,
            );
        }
        TableType::Struct => {
            let mut struct_buf: Vec<u8> = vec![];
            for (k, v) in val.try_as_object()?
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
                &val.try_as_bytes()?.to_vec(),
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

    pub fn map(&self, value: &Value) -> Result<Vec<u8>, TryTypeError> {
        let obj = value.try_as_object()?;
        let mut result = Vec::with_capacity(obj.len());

        for (key, val) in obj {
            let k: &str = key;
            if let Some(field) = self.fields.get(k) {
                encode_field(val, field, &mut result)?;
            }
        }
        Ok(result)
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
    ) -> anyhow::Result<SinkReply> {
        let request_size_limit = self.config.request_size_limit;

        let request_data = self
            .event_to_requests(event, ctx, request_size_limit)
            .await?;

        if request_data.len() > 1 {
            warn!("{ctx} The batch is too large to be sent in a single request, splitting it into {} requests. Consider lowering the batch size.", request_data.len());
        }

        let client = self
            .client
            .as_mut()
            .ok_or(GenericImplementationError::ClientNotAvailable("BigQuery"))?;
        for request in request_data {
            let req_timeout = Duration::from_nanos(self.config.request_timeout);
            let append_response =
                timeout(req_timeout, client.append_rows(stream::iter(vec![request]))).await;

            let Ok(append_response) = append_response else {
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

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
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
    ) -> anyhow::Result<Vec<AppendRowsRequest>> {
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
    ) -> anyhow::Result<&ConnectedWriteStream> {
        let client = self
            .client
            .as_mut()
            .ok_or(GenericImplementationError::ClientNotAvailable("BigQuery"))?;

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
                        .clone()
                        .ok_or(GbqError::GbqSchemaNotProvided(table_id))?
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
mod test;
