//! Implements the actual connector functionality.

use tremor_common::time::nanotime;
use tremor_pipeline::DEFAULT_STREAM_ID;
use tremor_runtime::{connectors::prelude::*, pdk::RResult, ttry, utils::hostname};
use tremor_script::{EventOriginUri, EventPayload};
use tremor_value::literal;

use std::{
    future,
    time::{Duration, Instant},
};

use abi_stable::{
    rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FutureExt};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Interval in milliseconds
    pub interval: u64,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
struct Metronome {
    interval: Duration,
    next: Instant,
    origin_uri: EventOriginUri,
}

impl RawConnector for Metronome {
    fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>> {
        // No connection is actually necessary, it's just local work
        future::ready(ROk(true)).into_ffi()
    }

    /// Exports the metronome as a source trait object
    fn create_source(
        &mut self,
        _ctx: SourceContext,
        _qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let metronome = self.clone();
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let source = BoxedRawSource::from_value(metronome, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

impl RawSource for Metronome {
    fn pull_data<'a>(
        &'a mut self,
        pull_id: &'a mut u64,
        _ctx: &'a SourceContext,
    ) -> BorrowingFfiFuture<'a, RResult<SourceReply>> {
        let now = Instant::now();
        let reply = if self.next < now {
            self.next = now + self.interval;
            let data = literal!({
                "onramp": "metronome",
                "ingest_ns": nanotime(),
                "id": *pull_id
            });
            // We need the pdk event payload, so we convert twice
            let data: EventPayload = data.into();
            SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: data.into(),
                stream: DEFAULT_STREAM_ID,
                port: RNone,
            }
        } else {
            let remaining = (self.next - now).as_millis() as u64;

            SourceReply::Empty(remaining)
        };

        future::ready(ROk(reply)).into_ffi()
    }

    fn is_transactional(&self) -> bool {
        false
    }
}

/// Configures and exports the metronome as a connector trait object
#[sabi_extern_fn]
pub fn from_config<'a>(
    _id: RStr<'a>,
    config: &'a ConnectorConfig,
) -> BorrowingFfiFuture<'a, RResult<BoxedRawConnector>> {
    async move {
        if let RSome(raw_config) = &config.config {
            let config = ttry!(Config::new(raw_config));

            let origin_uri = EventOriginUri {
                scheme: RString::from("tremor-metronome"),
                host: hostname().into(),
                port: RNone,
                path: rvec![config.interval.to_string().into()],
            };
            let metronome = Metronome {
                origin_uri,
                interval: Duration::from_millis(config.interval),
                next: Instant::now(),
            };

            ROk(BoxedRawConnector::from_value(metronome, TD_Opaque))
        } else {
            RErr(ErrorKind::MissingConfiguration(String::from("metronome")).into())
        }
    }
    .into_ffi()
}

#[sabi_extern_fn]
pub fn connector_type() -> ConnectorType {
    "metronome".into()
}
