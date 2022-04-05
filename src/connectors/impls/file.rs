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

use std::{ffi::OsStr, path::PathBuf};

use crate::connectors::prelude::*;
use async_compression::futures::bufread::XzDecoder;
use async_std::{
    fs::{File as FSFile, OpenOptions},
    io::BufReader,
};
use futures::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tremor_common::asy::file;
use tremor_pipeline::pdk::PdkEvent;

use crate::ttry;
use abi_stable::{
    prefix_type::PrefixTypeTrait,
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString, RVec,
    },
    type_level::downcasting::TD_Opaque,
    StableAbi,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt};
use async_std::channel::Sender;
use std::future;
use tremor_value::pdk::PdkValue;

const URL_SCHEME: &str = "tremor-file";

/// how to open the given file for writing
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Mode {
    /// read from file
    Read,
    /// equivalent to `truncate` only here because it has such a nice name
    Write,
    /// append to the file
    Append,
    /// truncate the file to 0 bytes and then write to it
    Truncate,
    /// just write to it and overwrite existing contents, do not truncate
    Overwrite,
}

impl Mode {
    fn as_open_options(&self) -> OpenOptions {
        let mut o = OpenOptions::new();
        match self {
            Self::Read => {
                o.read(true);
            }
            Self::Append => {
                o.create(true).write(true).append(true);
            }
            Self::Write | Self::Truncate => {
                o.create(true).write(true).truncate(true);
            }
            Self::Overwrite => {
                o.create(true).write(true);
            }
        }
        o
    }
}

/// File connector config
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// path to the file
    pub(crate) path: PathBuf,
    /// how to interface with the file
    pub(crate) mode: Mode, // whether we read or write (in various forms)
    /// chunk_size to read from the file
    #[serde(default = "default_buf_size")]
    pub(crate) chunk_size: usize,
}

impl ConfigImpl for Config {}

/// file connector
pub(crate) struct File {
    config: Config,
}

/// Note that since it's a built-in plugin, `#[export_root_module]` can't be
/// used or it would conflict with other plugins.
pub fn instantiate_root_module() -> ConnectorMod_Ref {
    ConnectorMod {
        connector_type,
        from_config,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
fn connector_type() -> ConnectorType {
    "file".into()
}

#[sabi_extern_fn]
pub fn from_config(
    id: RString,
    config: ConnectorConfig,
) -> FfiFuture<RResult<BoxedRawConnector>> {
    async move {
        if let RSome(raw_config) = config.config {
            let config = ttry!(Config::new(&raw_config.into()));
            let file = File { config };
            ROk(BoxedRawConnector::from_value(file, TD_Opaque))
        } else {
            RErr(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
    .into_ffi()
}

impl RawConnector for File {
    fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        qsize: usize,
        reply_tx: BoxedContraflowSender,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSink>>> {
        let sink = if self.config.mode == Mode::Read {
            RNone
        } else {
            let sink = FileSink::new(self.config.clone());
            // We don't need to be able to downcast the connector back to the
            // original type, so we just pass it as an opaque type.
            let sink = BoxedRawSink::from_value(sink, TD_Opaque);
            RSome(sink)
        };

        future::ready(ROk(sink)).into_ffi()
    }

    fn create_source(
        &mut self,
        source_context: SourceContext,
        qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let source = if self.config.mode == Mode::Read {
            RSome(FileSource::new(self.config.clone()))
        } else {
            RNone
        };
        let source = BoxedRawSource::from_value(source, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    /*
    async fn connect(&mut self, ctx: &ConnectorContext, attempt: &Attempt) -> Result<bool> {
        // SINK PART: open write file
        if let Some(sink_runtime) = self.sink_runtime.as_ref() {
            let mode = if attempt.is_first() || attempt.success() == 0 {
                &self.config.mode
            } else {
                // if we have already opened the file successfully once
                // we should not truncate it again or overwrite, but indeed append
                // otherwise the reconnect logic will lead to unwanted effects
                // e.g. if a simple write failed temporarily
                &Mode::Append
            };
            let write_file =
                file::open_with(&self.config.path, &mut mode.as_open_options()).await?;
            let writer = FileWriter::new(write_file, ctx.alias.clone());
            sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, writer);
        }
        // SOURCE PART: open file for reading
        // open in read-only mode, without creating a new one - will fail if the file is not available
        if let Some(source_runtime) = self.source_runtime.as_ref() {
            let meta = ctx.meta(literal!({
                "path": self.config.path.display().to_string()
            }));
            let read_file =
                file::open_with(&self.config.path, &mut self.config.mode.as_open_options()).await?;
            // TODO: instead of looking for an extension
            // check the magic bytes at the beginning of the file to determine the compression applied
            if let Some("xz") = self.config.path.extension().and_then(OsStr::to_str) {
                let reader = FileReader::xz(
                    read_file,
                    self.config.chunk_size,
                    ctx.alias.clone(),
                    self.origin_uri.clone(),
                    meta,
                );
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
            } else {
                let reader = FileReader::new(
                    read_file,
                    self.config.chunk_size,
                    ctx.alias.clone(),
                    self.origin_uri.clone(),
                    meta,
                );
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
            };
        }
        Ok(true)
    }*/

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct FileSource {
    config: Config,
    reader: Option<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    underlying_file: Option<FSFile>,
    buf: Vec<u8>,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    eof: bool,
}

impl FileSource {
    fn new(config: Config) -> Self {
        let buf = vec![0; config.chunk_size];
        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.into(),
            host: hostname().into(),
            port: RNone,
            path: rvec![config.path.display().to_string().into()],
        };
        Self {
            config,
            reader: None,
            underlying_file: None,
            buf,
            origin_uri,
            meta: Value::null(), // dummy value, will be overwritten in connect
            eof: false,
        }
    }
}

#[async_trait::async_trait]
impl RawSource for FileSource {
    fn connect(
        &mut self,
        ctx: &SourceContext,
        _attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>> {
        async move {
            self.meta = ctx.meta(literal!({
                "path": self.config.path.display().to_string()
            }));
            let read_file =
                file::open_with(&self.config.path, &mut self.config.mode.as_open_options())
                    .await
                    .into()?;
            // TODO: instead of looking for an extension
            // check the magic bytes at the beginning of the file to determine the compression applied
            if let Some("xz") = self.config.path.extension().and_then(OsStr::to_str) {
                self.reader = Some(Box::new(XzDecoder::new(BufReader::new(read_file.clone()))));
                self.underlying_file = Some(read_file);
            } else {
                    SourceReply::Data {
                        origin_uri: self.origin_uri.clone(),
                        stream: RSome(DEFAULT_STREAM_ID),
                        meta: RSome(self.meta.clone()),
                        // ALLOW: with the read above we ensure that this access is valid, unless async_std is broken
                        data: RVec::from(self.buf[0..bytes_read]),
                        port: RSome(OUT),
                        codec_overwrite: RNone,
                    }
                }
            };
            ROk(reply)
        }
        .into_ffi()
    }

    fn pull_data<'a>(
        &'a mut self,
        pull_id: &'a mut u64,
        ctx: &'a SourceContext,
    ) -> BorrowingFfiFuture<'a, RResult<SourceReply>> {
        async move {
            let reply = if self.eof {
                SourceReply::Empty(DEFAULT_POLL_INTERVAL)
            } else {
                let reader = self
                    .reader
                    .as_mut()
                    .ok_or_else(|| Error::from("No file available."))?;
                let bytes_read = reader.read(&mut self.buf).await?;
                if bytes_read == 0 {
                    self.eof = true;
                    debug!("{} EOF", &ctx);
                    SourceReply::EndStream {
                        origin_uri: self.origin_uri.clone(),
                        stream: DEFAULT_STREAM_ID,
                        meta: Some(self.meta.clone()),
                    }
                } else {
                    SourceReply::Data {
                        origin_uri: self.origin_uri.clone(),
                        stream: DEFAULT_STREAM_ID,
                        meta: Some(self.meta.clone()),
                        // ALLOW: with the read above we ensure that this access is valid, unless async_std is broken
                        data: self.buf[0..bytes_read].to_vec(),
                        port: Some(OUT),
                    }
                }
            };
            ROk(reply)
        }
        .into_ffi()
    }

    fn on_stop(&mut self, ctx: &SourceContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move {
            if let Some(mut file) = self.underlying_file.take() {
                if let Err(e) = file.close().await {
                    error!("{} Error closing file: {}", &ctx, e);
                }
            }
            ROk(())
        }
        .into_ffi()
    }

    fn is_transactional(&self) -> bool {
        // TODO: we could make the file source transactional
        // by recording the current position into the file after every read
        false
    }

    fn asynchronous(&self) -> bool {
        // this one is special, in that we want it to
        // read until EOF before we consider this drained
        true
    }
}

struct FileSink {
    config: Config,
    file: Option<FSFile>,
}

impl FileSink {
    fn new(config: Config) -> Self {
        Self { config, file: None }
    }
}

impl RawSink for FileSink {
    fn connect(
        &mut self,
        _ctx: &SinkContext,
        attempt: &Attempt,
    ) -> BorrowingFfiFuture<'_, RResult<bool>> {
        async move {
            let mode = if attempt.is_first() || attempt.success() == 0 {
                &self.config.mode
            } else {
                // if we have already opened the file successfully once
                // we should not truncate it again or overwrite, but indeed append
                // otherwise the reconnect logic will lead to unwanted effects
                // e.g. if a simple write failed temporarily
                &Mode::Append
            };
            debug!(
                "[Sink::file] opening file {} with options {:?}",
                self.config.path.to_string_lossy(),
                mode
            );
            let file = file::open_with(&self.config.path, &mut mode.as_open_options()).await?;
            self.file = Some(file);
            ROk(true)
        }
        .into_ffi()
    }
    fn on_event<'a>(
        &'a mut self,
        input: RStr<'a>,
        event: PdkEvent,
        ctx: &'a SinkContext,
        serializer: &'a mut MutEventSerializer,
        start: u64,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>> {
        async move {
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| Error::from("No file available."))?;
            let ingest_ns = event.ingest_ns;
            for value in event.value_iter() {
                let data = serializer.serialize(value, ingest_ns)?;
                for chunk in data {
                    if let Err(e) = file.write_all(&chunk).await {
                        error!("{} Error writing to file: {}", &ctx, &e);
                        self.file = None;
                        ctx.notifier().notify().await?;
                        return RErr(e.into());
                    }
                }
                if let Err(e) = file.flush().await {
                    error!("{} Error flushing file: {}", &ctx, &e);
                    self.file = None;
                    ctx.notifier().connection_lost().await?;
                    return RErr(e.into());
                }
            }
            if let Err(e) = file.flush().await {
                error!("{} Error flushing file: {}", &ctx, &e);
                self.file = None;
                ctx.notifier().connection_lost().await?;
                return RErr(e.into());
            }
        }
        .into_ffi()
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }

    fn on_stop(&mut self, ctx: &SinkContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move {
            if let Some(file) = self.file.take() {
                if let Err(e) = file.sync_all().await {
                    error!("{} Error flushing file: {}", &ctx, e);
                }
            }
            ROk(())
        }
        .into_ffi()
    }
}
