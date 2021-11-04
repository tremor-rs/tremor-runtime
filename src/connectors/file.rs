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
use tremor_value::{literal, Value};

const URL_SCHEME: &str = "tremor-file";

/// how to open the given file for writing
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// read from file
    Read,
    /// Default write mode - equivalent to `truncate`
    /// only here because it has such a nice name
    Write,
    /// append to the file
    Append,
    /// truncate the file to 0 bytes and then write to it (Default)
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
pub struct Config {
    /// path to the file
    pub path: PathBuf,
    /// how to interface with the file
    pub mode: Mode, // whether we read or write (in various forms)
    /// chunk_size to read from the file
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
}

fn default_chunk_size() -> usize {
    // equals default chunk size for BufReader
    8 * 1024
}

impl ConfigImpl for Config {}

/// file connector
pub struct File {
    config: Config,
    origin_uri: EventOriginUri,
    source_runtime: Option<ChannelSourceRuntime>,
    sink_runtime: Option<SingleStreamSinkRuntime>,
}

/// builder for file connector
#[derive(Default)]
pub struct Builder {}

impl Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: hostname(),
                port: None,
                path: vec![config.path.display().to_string()],
            };
            Ok(Box::new(File {
                config,
                origin_uri,
                source_runtime: None,
                sink_runtime: None,
            }))
        } else {
            let mut id = id.clone();
            id.trim_to_instance();
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

#[async_trait::async_trait]
impl Connector for File {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: super::sink::SinkManagerBuilder,
    ) -> Result<Option<super::sink::SinkAddr>> {
        Ok(if self.config.mode == Mode::Read {
            None
        } else {
            let sink = SingleStreamSink::new_no_meta(builder.qsize(), builder.reply_tx());
            self.sink_runtime = Some(sink.runtime());
            let addr = builder.spawn(sink, sink_context)?;
            Some(addr)
        })
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: super::source::SourceManagerBuilder,
    ) -> Result<Option<super::source::SourceAddr>> {
        Ok(if self.config.mode == Mode::Read {
            let source = ChannelSource::new(source_context.clone(), builder.qsize());
            self.source_runtime = Some(source.runtime());
            let addr = builder.spawn(source, source_context)?;
            Some(addr)
        } else {
            None
        })
    }

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
            let writer = FileWriter::new(write_file, ctx.url.clone());
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
                    ctx.url.clone(),
                    self.origin_uri.clone(),
                    meta,
                );
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
            } else {
                let reader = FileReader::new(
                    read_file,
                    self.config.chunk_size,
                    ctx.url.clone(),
                    self.origin_uri.clone(),
                    meta,
                );
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
            };
        }

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

struct FileReader<R>
where
    R: AsyncRead + Send + Unpin,
{
    reader: R,
    underlying_file: FSFile,
    buf: Vec<u8>,
    url: TremorUrl,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
}

impl FileReader<FSFile> {
    fn new(
        file: FSFile,
        chunk_size: usize,
        url: TremorUrl,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
    ) -> Self {
        Self {
            reader: file.clone(),
            underlying_file: file,
            buf: vec![0; chunk_size],
            url,
            origin_uri,
            meta,
        }
    }
}

impl FileReader<XzDecoder<BufReader<FSFile>>> {
    fn xz(
        file: FSFile,
        chunk_size: usize,
        url: TremorUrl,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
    ) -> Self {
        Self {
            reader: XzDecoder::new(BufReader::new(file.clone())),
            underlying_file: file,
            buf: vec![0; chunk_size],
            url,
            origin_uri,
            meta,
        }
    }
}

#[async_trait::async_trait]
impl<R> StreamReader for FileReader<R>
where
    R: AsyncRead + Send + Unpin,
{
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
        let bytes_read = self.reader.read(&mut self.buf).await?;
        Ok(if bytes_read == 0 {
            trace!("[Connector::{}] EOF", &self.url);
            SourceReply::EndStream(stream)
        } else {
            SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                stream,
                meta: Some(self.meta.clone()),
                data: self.buf[0..bytes_read].to_vec(),
                port: None,
            }
        })
    }

    async fn on_done(&mut self, _stream: u64) -> StreamDone {
        if let Err(e) = self.underlying_file.close().await {
            error!("[Connector::{}] Error closing file: {}", &self.url, e);
        }
        // we do not use ConnectorClosed - as we don't want to trigger a reconnect
        // which would read the whole file again
        StreamDone::StreamClosed
    }
}

struct FileWriter {
    file: FSFile,
    url: TremorUrl,
}

impl FileWriter {
    fn new(file: FSFile, url: TremorUrl) -> Self {
        Self { file, url }
    }
}

#[async_trait::async_trait]
impl StreamWriter for FileWriter {
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<SinkMeta>) -> Result<()> {
        for chunk in data {
            self.file.write_all(&chunk).await?;
        }
        self.file.flush().await?;
        Ok(())
    }

    async fn on_done(&self, _stream: u64) -> Result<StreamDone> {
        if let Err(e) = self.file.sync_all().await {
            error!("[Connector::{}] Error flushing file: {}", &self.url, e);
        }
        // if we cannot write to the given file anymore
        // something wen't really wrong, a reconnect might help here
        // this wont lead to overwriting stuff or re-truncating a file or so
        // as we always use `Append` in that case
        Ok(StreamDone::ConnectorClosed)
    }
}
