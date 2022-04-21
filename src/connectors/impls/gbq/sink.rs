use async_std::prelude::StreamExt;
use futures::stream;
use googapis::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use googapis::google::cloud::bigquery::storage::v1::{append_rows_request, AppendRowsRequest, CreateWriteStreamRequest, ProtoRows, ProtoSchema, write_stream, WriteStream};
use googapis::google::cloud::bigquery::storage::v1::append_rows_request::ProtoData;
use prost::encoding::WireType;
use prost_types::{DescriptorProto, field_descriptor_proto, FieldDescriptorProto};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status};
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use crate::connectors::impls::gbq::Config;
use crate::connectors::prelude::*;

pub(crate) struct GbqSink {
    client: BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>,
    write_stream: WriteStream
}

pub(crate) struct AuthInterceptor {
    token: MetadataValue<Ascii>
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        request.metadata_mut()
            .insert("authorization", self.token.clone());

        Ok(request)
    }
}

impl GbqSink where {
    pub async fn new(config:Config) -> Result<Self> {
        let token_metadata_value = MetadataValue::from_str(format!("Bearer {}", config.token).as_str()).unwrap();

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("pubsub.googleapis.com");

        let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
            .tls_config(tls_config)?
            .connect()
            .await?;

        let mut client = BigQueryWriteClient::with_interceptor(channel, AuthInterceptor { token: token_metadata_value });

        let write_stream = client.create_write_stream(CreateWriteStreamRequest {
            parent: config.table_id.clone(),
            write_stream: Some(WriteStream {
                name: "".to_string(),
                r#type: i32::from(write_stream::Type::Committed),
                create_time: None,
                commit_time: None,
                table_schema: None
            }) }).await?.into_inner();

        Ok(
            Self {
                client,
                write_stream
            }
        )
    }
}

#[async_trait::async_trait]
impl Sink for GbqSink {
    async fn on_event(&mut self, _input: &str, _event: Event, _ctx: &SinkContext, _serializer: &mut EventSerializer, _start: u64) -> Result<SinkReply> {
        let mut buf = vec![];
        prost::encoding::encode_key(1, WireType::Varint, &mut buf);
        prost::encoding::encode_varint(356, &mut buf);

        let request = AppendRowsRequest {
            write_stream: self.write_stream.name.clone(),
            offset: None,
            trace_id: "".to_string(),
            rows: Some(
                append_rows_request::Rows::ProtoRows
                    (
                        ProtoData
                        {
                            writer_schema: Some(ProtoSchema{ proto_descriptor: Some(DescriptorProto{
                                name: Some("row".to_string()),
                                field: vec![FieldDescriptorProto{
                                    name: Some("x1".to_string()),
                                    number: Some(1),
                                    label: None,
                                    r#type: Some(i32::from(field_descriptor_proto::Type::Int64)),
                                    type_name: None,
                                    extendee: None,
                                    default_value: None,
                                    oneof_index: None,
                                    json_name: None,
                                    options: None,
                                    proto3_optional: None
                                }],
                                extension: vec![],
                                nested_type: vec![],
                                enum_type: vec![],
                                extension_range: vec![],
                                oneof_decl: vec![],
                                options: None,
                                reserved_range: vec![],
                                reserved_name: vec![]
                            }) }),
                            rows: Some(ProtoRows{ serialized_rows: vec![buf] })
                        }
                    )
            )
        };

        let mut apnd_response = self.client.append_rows(stream::iter(vec![request])).await.unwrap().into_inner();
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
