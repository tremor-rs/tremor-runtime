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

//! The file connector enables reading and writing to files accessible via the file system.
//!
//!
//! ## Configuration
//!
//! File based event processint can be read oriented by setting the `mode` configuration
//! field to `read` or write based by setting the mode to one of:
//!
//! * `write` or `truncate` - Creates or Opens and truncates the target file
//! * `append` - Creates or Opens the target file without truncation
//! * `overwrite` - Creates or Opens without truncation and overwrites existing data
//!
//! ### Reader
//!
//! An example file reader
//!
//! ```tremor
//!   define connector in_file from file
//!   with
//!     codec = "json"
//!     preprocessors = ["separate"], # line by line processing
//!     config = {
//!       "path": "in.json",
//!       "mode": "read",
//!
//!       # Override the default builtin internal buffer chunk size
//!       # chunk_size = 8192,
//!     },
//!   end;
//! ```
//!
//! Other useful `codec` options might be `base64`, `string` or `influx`.
//!
//!
//! ### Writer
//!
//! An example file writer
//!
//! ```tremor
//!   define connector out_file from file
//!   with
//!     codec = "json-sorted", # Json sorted creates a consistent field order in record types
//!     postprocessors = ["separate"], # line by line processing
//!     config = {
//!       "path": "out.log",
//!       "mode": "truncate",
//!
//!       # Override the default builtin internal buffer chunk size
//!       # chunk_size = 8192,
//!     },
//!   end;
//!
//!
//!
//! ## How do I copy files with tremor?
//!
//! ```mermaid
//! graph LR
//!     A{File: in.json} -->|read events line by line| B(passthrough)
//!     B -->|write events line by line| C{File: out.json}
//! ```
//!
//! For this use case we need a file reader and writer
//!
//! ```tremor
//! define flow main
//! flow  
//!   use tremor::connectors;
//!   use integration;
//!
//!   # we could use the connectors in integration but this is the file integration test
//!   # so it makes more sense to define them here
//!   define connector out_file from file
//!   with
//!     codec = "json-sorted", # Json sorted creates a consistent field order in record types
//!     postprocessors = ["separate"], # line by line processing
//!     config = {
//!       "path": "out.log",
//!       "mode": "truncate"
//!     },
//!   end;
//!
//!   define connector in_file from file
//!   with
//!     codec = "json",
//!     preprocessors = ["separate"], # line by line processing
//!     config = {
//!       "path": "in.json",
//!       "mode": "read"
//!     },
//!   end;
//!
//!   create connector in_file;
//!   create connector out_file;
//!   create connector exit from connectors::exit;
//!   create pipeline main from integration::out_or_exit;
//!
//!   connect /connector/in_file to /pipeline/main;
//!   connect /pipeline/main to /connector/out_file;
//!   connect /pipeline/main/exit to /connector/exit;
//!   
//! end;
//! deploy flow main;
//! ```
//!
//! ## How do I transform files with tremor?
//!
//! For example we can read a line delimited JSON file and write it as base64 encoded YAML
//! by changing our copy example above to use a different file writer configuration:
//!
//! ```tremor
//!   define connector out_file from file
//!   with
//!     codec = "yaml",
//!     postprocessors = ["base64", "separate"], # line by line processing
//!     config = {
//!       "path": "out.log",
//!       "mode": "truncate"
//!     },
//!   end;
//! ```

use std::{ffi::OsStr, path::PathBuf};

use crate::connectors::prelude::*;
use async_compression::tokio::bufread::XzDecoder;
use tokio::{
    fs::{File as FSFile, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
};
use tremor_common::asy::file;

const URL_SCHEME: &str = "tremor-file";

/// how to open the given file for writing
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
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

impl tremor_config::Impl for Config {}

/// file connector
pub(crate) struct File {
    config: Config,
}

/// builder for file connector
#[derive(Default, Debug)]
pub(crate) struct Builder {}

impl Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "file".into()
    }

    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        Ok(Box::new(File { config }))
    }
}

#[async_trait::async_trait]
impl Connector for File {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        if self.config.mode == Mode::Read {
            Ok(None)
        } else {
            let sink = FileSink::new(self.config.clone());
            Ok(Some(builder.spawn(sink, ctx)))
        }
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        if self.config.mode == Mode::Read {
            let source = FileSource::new(self.config.clone());
            Ok(Some(builder.spawn(source, ctx)))
        } else {
            Ok(None)
        }
    }

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
            scheme: URL_SCHEME.to_string(),
            host: hostname(),
            port: None,
            path: vec![config.path.display().to_string()],
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

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("No file available")]
    NoFile,
}

#[async_trait::async_trait]
impl Source for FileSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        self.meta = ctx.meta(literal!({
            "path": self.config.path.display().to_string()
        }));
        let read_file =
            file::open_with(&self.config.path, &mut self.config.mode.as_open_options()).await?;
        // TODO: instead of looking for an extension
        // check the magic bytes at the beginning of the file to determine the compression applied
        if let Some("xz") = self.config.path.extension().and_then(OsStr::to_str) {
            self.reader = Some(Box::new(XzDecoder::new(BufReader::new(
                read_file.try_clone().await?,
            ))));
            self.underlying_file = Some(read_file);
        } else {
            self.reader = Some(Box::new(read_file.try_clone().await?));
            self.underlying_file = Some(read_file);
        };
        Ok(true)
    }
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        let reply = if self.eof {
            SourceReply::Finished
        } else {
            let reader = self.reader.as_mut().ok_or(Error::NoFile)?;
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
                    stream: Some(DEFAULT_STREAM_ID),
                    meta: Some(self.meta.clone()),
                    // ALLOW: with the read above we ensure that this access is valid, unless AsyncRead is broken
                    data: self.buf[0..bytes_read].to_vec(),
                    port: Some(OUT),
                    codec_overwrite: None,
                }
            }
        };
        Ok(reply)
    }

    async fn on_stop(&mut self, ctx: &SourceContext) -> anyhow::Result<()> {
        if let Some(mut file) = self.underlying_file.take() {
            if let Err(e) = file.flush().await {
                error!("{} Error flushing file: {}", &ctx, e);
            }
            if let Err(e) = file.sync_all().await {
                error!("{} Error syncing file: {}", &ctx, e);
            }
        }
        Ok(())
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

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn connect(&mut self, _ctx: &SinkContext, attempt: &Attempt) -> anyhow::Result<bool> {
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
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let file = self.file.as_mut().ok_or(Error::NoFile)?;
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, ingest_ns).await?;
            for chunk in data {
                if let Err(e) = file.write_all(&chunk).await {
                    error!("{ctx} Error writing to file: {e}");
                    self.file = None;
                    ctx.notifier().connection_lost().await?;
                    return Err(e.into());
                }
            }
            if let Err(e) = file.flush().await {
                error!("{ctx} Error flushing file: {e}");
                self.file = None;
                ctx.notifier().connection_lost().await?;
                return Err(e.into());
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> anyhow::Result<()> {
        if let Some(file) = self.file.take() {
            if let Err(e) = file.sync_all().await {
                error!("{} Error flushing file: {}", &ctx, e);
            }
        }
        Ok(())
    }
}
