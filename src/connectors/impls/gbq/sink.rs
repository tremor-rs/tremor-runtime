use std::collections::HashMap;
use crate::connectors::impls::gbq::Config;
use crate::connectors::prelude::*;
use async_std::prelude::StreamExt;
use futures::stream;
use googapis::google::cloud::bigquery::storage::v1::append_rows_request::ProtoData;
use googapis::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use googapis::google::cloud::bigquery::storage::v1::{append_rows_request, write_stream, AppendRowsRequest, CreateWriteStreamRequest, ProtoRows, ProtoSchema, WriteStream, TableFieldSchema, table_field_schema};
use googapis::google::cloud::bigquery::storage::v1::table_field_schema::Type as TableType;
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use prost_types::field_descriptor_proto::Type;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Status};

pub(crate) struct GbqSink {
    client: BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>,
    write_stream: WriteStream,
    mapping: JsonToProtobufMapping
}

pub(crate) struct AuthInterceptor {
    token: MetadataValue<Ascii>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());

        Ok(request)
    }
}

struct Field {
    grpc_type: field_descriptor_proto::Type,
    tag: u32
}

struct JsonToProtobufMapping {
    fields: HashMap<String, Field>,
    descriptor: DescriptorProto
}

impl JsonToProtobufMapping {
    pub fn new(vec: &Vec<TableFieldSchema>) -> Self {
        let mut fields = HashMap::new();
        let mut proto_fields = vec![];

        let mut tag:u32 = 1;
        for raw_field in vec.iter() {
            let r#type = match table_field_schema::Type::from_i32(raw_field.r#type) {
                None => todo!("Unknown field type"),
                Some(table_field_schema::Type::Int64) => field_descriptor_proto::Type::Int64,
                Some(TableType::String) => field_descriptor_proto::Type::String,
                Some(TableType::Double) => field_descriptor_proto::Type::Double,
                Some(TableType::Bool) => field_descriptor_proto::Type::Bool,
                Some(TableType::Bytes) => field_descriptor_proto::Type::Bytes,

                Some(TableType::Date) |
                Some(TableType::Time) |
                Some(TableType::Datetime) |
                Some(TableType::Geography) |
                Some(TableType::Numeric) |
                Some(TableType::Bignumeric) |
                Some(TableType::Interval) |
                Some(TableType::Json) | Some(TableType::Timestamp) | Some(TableType::Unspecified) | Some(TableType::Struct) => todo!("Unsupported field type")
            };

            proto_fields.push(FieldDescriptorProto {
                name: Some(raw_field.name.to_string()),
                number: Some(i32::try_from(tag).unwrap()),
                label: None,
                r#type: Some(i32::from(r#type)),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: None,
                options: None,
                proto3_optional: None,
            });

            fields.insert(raw_field.name.to_string(), Field {tag, grpc_type: r#type });

            tag += 1;
        }

        let descriptor = DescriptorProto {
            name: Some("row".to_string()),
            field: proto_fields,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };

        Self {
            descriptor,
            fields
        }
    }

    pub fn map(&self, value:Value) -> Vec<u8> {
        let mut result = vec![];
        if let Some(obj) = value.as_object() {
            for (key, val) in obj {
                if let Some(field) = self.fields.get(&key.to_string()) {
                    let tag = field.tag;

                    // fixme check which fields are required and fail if they're missing
                    // fixme do not panic if the tremor type does not match
                    match field.grpc_type {
                        Type::Double => prost::encoding::double::encode(tag, &val.as_f64().unwrap(), &mut result),
                        Type::Float => prost::encoding::float::encode(tag, &val.as_f32().unwrap(), &mut result),
                        Type::Int64 => prost::encoding::int64::encode(tag, &val.as_i64().unwrap(), &mut result),
                        Type::Uint64 => prost::encoding::uint64::encode(tag, &val.as_u64().unwrap(), &mut result),
                        Type::Int32 => prost::encoding::int32::encode(tag, &val.as_i32().unwrap(), &mut result),
                        Type::Uint32 => prost::encoding::uint32::encode(tag, &val.as_u32().unwrap(), &mut result),
                        Type::Bool => prost::encoding::bool::encode(tag, &val.as_bool().unwrap(), &mut result),
                        Type::String => prost::encoding::string::encode(tag, &val.as_str().unwrap().to_string(), &mut result),
                        Type::Bytes | Type::Sint64 | Type::Sint32 | Type::Sfixed64 | Type::Sfixed32 | Type::Enum | Type::Message | Type::Group | Type::Fixed32 | Type::Fixed64 => todo!("This type is not supported"),
                    }
                }
            }
        }

        result
    }

    pub fn descriptor(&self) -> &DescriptorProto { &self.descriptor }
}

impl GbqSink {
    pub async fn new(config: Config) -> Result<Self> {
        let token_metadata_value =
            MetadataValue::from_str(format!("Bearer {}", config.token).as_str()).unwrap();

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("pubsub.googleapis.com");

        let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
            .tls_config(tls_config)?
            .connect()
            .await?;

        let mut client = BigQueryWriteClient::with_interceptor(
            channel,
            AuthInterceptor {
                token: token_metadata_value,
            },
        );

        let write_stream = client
            .create_write_stream(CreateWriteStreamRequest {
                parent: config.table_id.clone(),
                write_stream: Some(WriteStream {
                    name: "".to_string(),
                    r#type: i32::from(write_stream::Type::Committed),
                    create_time: None,
                    commit_time: None,
                    table_schema: None,
                }),
            })
            .await?
            .into_inner();

        let mapping = JsonToProtobufMapping::new(&write_stream.table_schema.as_ref().unwrap().clone().fields);
        Ok(Self {
            client,
            write_stream,
            mapping
        })
    }
}

#[async_trait::async_trait]
impl Sink for GbqSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let request = AppendRowsRequest {
            write_stream: self.write_stream.name.clone(),
            offset: None,
            trace_id: "".to_string(),
            rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
                writer_schema: Some(ProtoSchema {
                    proto_descriptor: Some(self.mapping.descriptor().clone()),
                }),
                rows: Some(ProtoRows {
                    serialized_rows: vec![self.mapping.map(event.data.parts().0.clone())],
                }),
            })),
        };

        let mut apnd_response = self
            .client
            .append_rows(stream::iter(vec![request]))
            .await?
            .into_inner();

        while let Some(x) = apnd_response.next().await {
            error!("{:?}", x);
        }

        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        // FIXME we should NOT do auto ack, instead ACK when we get the response from GBQ
        true
    }
}
