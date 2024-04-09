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

use super::meta;
use crate::gcl::writer::Config;
use crate::utils::AuthInterceptor;
use crate::utils::ChannelFactory;
use googapis::google::logging::v2::{
    log_entry::Payload, logging_service_v2_client::LoggingServiceV2Client, LogEntry,
    WriteLogEntriesRequest,
};
use log::{error, info};
use std::time::Duration;
use tokio::time::timeout;
use tonic::{
    codegen::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig},
    Code,
};
use tremor_common::time::nanotime;
use tremor_connectors::{
    sink::{concurrency_cap::ConcurrencyCap, prelude::*},
    spawn_task,
    utils::pb,
};
use tremor_value::prelude::*;

pub(crate) struct TonicChannelFactory;

#[async_trait::async_trait]
impl ChannelFactory<Channel> for TonicChannelFactory {
    async fn make_channel(&self, connect_timeout: Duration) -> anyhow::Result<Channel> {
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("logging.googleapis.com");

        Ok(Channel::from_static("https://logging.googleapis.com")
            .connect_timeout(connect_timeout)
            .tls_config(tls_config)?
            .connect()
            .await?)
    }
}

pub(crate) struct GclSink<TChannel>
where
    TChannel: tonic::codegen::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::transport::Body>,
        > + Clone,
{
    client: Option<LoggingServiceV2Client<InterceptedService<TChannel, AuthInterceptor>>>,
    config: Config,
    concurrency_cap: ConcurrencyCap,
    reply_tx: ReplySender,
    channel_factory: Box<dyn ChannelFactory<TChannel> + Send + Sync>,
}

fn value_to_log_entry(
    timestamp: u64,
    config: &Config,
    data: &Value,
    meta: Option<&Value>,
) -> Result<LogEntry, TryTypeError> {
    Ok(LogEntry {
        log_name: config.log_name(meta),
        resource: super::value_to_monitored_resource(config.resource.as_ref())?,
        timestamp: Some(meta::timestamp(timestamp, meta)),
        receive_timestamp: None,
        severity: config.log_severity(meta)?,
        insert_id: meta::insert_id(meta),
        http_request: meta::http_request(meta),
        labels: Config::labels(meta),
        operation: meta::operation(meta),
        trace: meta::trace(meta),
        span_id: meta::span_id(meta),
        trace_sampled: meta::trace_sampled(meta)?,
        source_location: meta::source_location(meta),
        payload: Some(Payload::JsonPayload(pb::value_to_prost_struct(data)?)),
    })
}

impl<
        TChannel: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::transport::Body>,
                Error = TChannelError,
            > + Send
            + Clone,
        TChannelError: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    > GclSink<TChannel>
{
    pub fn new(
        config: Config,
        reply_tx: ReplySender,
        channel_factory: impl ChannelFactory<TChannel> + Send + Sync + 'static,
    ) -> Self {
        let concurrency_cap = ConcurrencyCap::new(config.concurrency, reply_tx.clone());
        Self {
            client: None,
            config,
            concurrency_cap,
            reply_tx,
            channel_factory: Box::new(channel_factory),
        }
    }
}

#[async_trait::async_trait]
impl<
        TChannel: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::transport::Body>,
                Error = TChannelError,
            > + Send
            + Clone
            + 'static,
        TChannelError: Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync,
    > Sink for GclSink<TChannel>
where
    TChannel::Future: Send,
{
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        start: u64,
    ) -> anyhow::Result<SinkReply> {
        let client = self
            .client
            .as_mut()
            .ok_or(GenericImplementationError::ClientNotAvailable(
                "Google Cloud Logging",
            ))?;

        let mut entries = Vec::with_capacity(event.len());
        for (data, meta) in event.value_meta_iter() {
            let meta = meta.get("gcl_writer").or(None);
            entries.push(value_to_log_entry(
                event.ingest_ns,
                &self.config,
                data,
                meta,
            )?);
        }

        let reply_tx = self.reply_tx.clone();
        let guard = self.concurrency_cap.inc_for(&event)?;
        let log_name = self.config.log_name(None);
        let resource = super::value_to_monitored_resource(self.config.resource.as_ref())?;
        let labels = self.config.labels.clone();
        let partial_success = self.config.partial_success;
        let dry_run = self.config.dry_run;
        let request_timeout = Duration::from_nanos(self.config.request_timeout);
        let task_ctx = ctx.clone();
        let mut task_client = client.clone();

        spawn_task(ctx.clone(), async move {
            let log_entries_response = timeout(
                request_timeout,
                task_client.write_log_entries(WriteLogEntriesRequest {
                    log_name,
                    resource,
                    labels,
                    entries,
                    partial_success,
                    dry_run,
                }),
            )
            .await?;

            if let Err(error) = log_entries_response {
                error!("Failed to write a log entries: {}", error);

                if matches!(
                    error.code(),
                    Code::Aborted
                        | Code::Cancelled
                        | Code::DataLoss
                        | Code::DeadlineExceeded
                        | Code::Internal
                        | Code::ResourceExhausted
                        | Code::Unavailable
                        | Code::Unknown
                ) {
                    task_ctx.swallow_err(
                        task_ctx.notifier().connection_lost().await,
                        "Failed to notify about Google Cloud Logging connection loss",
                    );
                }

                reply_tx.send(AsyncSinkReply::Fail(ContraflowData::from(event)))?;
            } else {
                reply_tx.send(AsyncSinkReply::Ack(
                    ContraflowData::from(event),
                    nanotime() - start,
                ))?;
            }

            drop(guard);
            Ok(())
        });

        Ok(SinkReply::NONE)
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        info!("{} Connecting to Google Cloud Logging", ctx);
        let channel = self
            .channel_factory
            .make_channel(Duration::from_nanos(self.config.connect_timeout))
            .await?;

        let client =
            LoggingServiceV2Client::with_interceptor(channel, self.config.token.clone().into());

        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test;
