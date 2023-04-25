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
    connectors::{prelude::*, traits::FileMode},
    errors::{already_created_error, empty_error},
};
use async_compression::tokio::bufread::XzDecoder;
use dashmap::DashMap;
use std::{ffi::OsStr, path::Path, sync::atomic::AtomicU64};
use tokio::{
    fs::File as FSFile,
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

/// File connector config
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// chunk_size to read from the file
    #[serde(default = "default_buf_size")]
    pub(crate) chunk_size: usize,

    pub(crate) default_handle: Option<String>,
}

impl ConfigImpl for Config {}

/// file connector
pub(crate) struct File {
    config: Config,
    files_tx: Sender<FileDescriptor>,
    files_rx: Option<Receiver<FileDescriptor>>,
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
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let (files_tx, files_rx) = crate::channel::bounded(qsize());
        Ok(Box::new(File {
            config,
            files_tx,
            files_rx: Some(files_rx),
        }))
    }
}

const IN_PORTS: [Port<'static>; 2] = [IN, CONTROL];
const IN_PORTS_REF: &[Port<'static>; 2] = &IN_PORTS;

#[async_trait::async_trait]
impl Connector for File {
    fn input_ports(&self) -> &[Port<'static>] {
        IN_PORTS_REF
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let rx = self.files_rx.take().ok_or_else(already_created_error)?;
        let source = FileSource::new(&self.config, rx);
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = FileSink::new(&self.config, self.files_tx.clone());

        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for cmd in &config.initial_commands {
            match structurize::<Command>(cmd.clone())? {
                Command::FileIo(file_io_cmd) => match file_io_cmd {
                    FileIoCommand::Open { .. } | FileIoCommand::Close { .. } => {}
                },
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "only file io commands are supported".into(),
                    )
                    .into())
                }
            }
        }

        Ok(())
    }
}

struct FileDescriptor {
    path: String,
    file: Option<FSFile>,
    stream_id: u64,
}

struct FileSource {
    reader: Option<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    current_file: Option<FileDescriptor>,
    files_to_read: Receiver<FileDescriptor>,
    buf: Vec<u8>,
}

impl FileSource {
    fn new(config: &Config, files_to_read: Receiver<FileDescriptor>) -> Self {
        let buf = vec![0; config.chunk_size];
        Self {
            reader: None,
            current_file: None,
            files_to_read,
            buf,
        }
    }
}

#[async_trait::async_trait]
impl Source for FileSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }
    async fn pull_data(&mut self, _pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        if self.reader.is_none() {
            let mut file = self.files_to_read.recv().await.ok_or_else(empty_error)?;
            if let Some("xz") = Path::new(&file.path).extension().and_then(OsStr::to_str) {
                self.reader = Some(Box::new(XzDecoder::new(BufReader::new(
                    file.file.take().ok_or("no file")?,
                ))));
                self.current_file = Some(file);
            } else {
                self.reader = Some(Box::new(BufReader::new(file.file.take().ok_or("no file")?)));
                self.current_file = Some(file);
            }
        }

        let current_file_path = self
            .current_file
            .as_ref()
            .map_or_else(String::new, |f| f.path.clone());
        let current_stream_id = self
            .current_file
            .as_ref()
            .map_or(DEFAULT_STREAM_ID, |f| f.stream_id);

        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: hostname(),
            port: None,
            path: vec![current_file_path.clone()],
        };

        let meta = ctx.meta(literal!({ "path": current_file_path }));

        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| Error::from("No file available."))?;

        let bytes_read = reader.read(&mut self.buf).await?;
        let reply = if bytes_read == 0 {
            self.reader = None;

            debug!("{} EOF", &ctx);

            SourceReply::EndStream {
                origin_uri: origin_uri.clone(),
                stream: current_stream_id,
                meta: Some(meta),
            }
        } else {
            SourceReply::Data {
                origin_uri: origin_uri.clone(),
                stream: Some(current_stream_id),
                meta: Some(meta),
                // ALLOW: with the read above we ensure that this access is valid, unless async_std is broken
                data: self.buf[0..bytes_read].to_vec(),
                port: Some(OUT),
                codec_overwrite: None,
            }
        };

        Ok(reply)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.reader = None;
        self.current_file = None;

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

#[derive(SocketServer, SocketClient, QueueSubscriber, DatabaseWriter)]
struct FileSink {
    file: Option<FileDescriptor>,
    files_to_read: Sender<FileDescriptor>,
    files_to_write: DashMap<String, FileDescriptor>,
    default_handle: Option<String>,
}

impl FileSink {
    fn new(config: &Config, files_to_read: Sender<FileDescriptor>) -> Self {
        Self {
            file: None,
            files_to_read,
            files_to_write: DashMap::new(),
            default_handle: config.default_handle.clone(),
        }
    }
}

static STREAM_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn open_file(path: &str, mode: FileMode) -> Result<FileDescriptor> {
    let descriptor = match mode {
        FileMode::Read => {
            let open_file = file::open_with(path, &mut mode.as_open_options()).await?;

            FileDescriptor {
                path: path.to_string(),
                file: Some(open_file),
                stream_id: STREAM_ID_COUNTER.fetch_add(1, Ordering::SeqCst),
            }
        }
        FileMode::WriteAppend | FileMode::WriteTruncate | FileMode::Overwrite => FileDescriptor {
            path: path.to_string(),
            file: Some(file::open_with(path, &mut mode.as_open_options()).await?),
            stream_id: STREAM_ID_COUNTER.fetch_add(1, Ordering::SeqCst),
        },
    };

    Ok(descriptor)
}

#[async_trait::async_trait]
impl FileIo for FileSink {
    async fn open(
        &mut self,
        path: &str,
        handle: &str,
        mode: FileMode,
        _ctx: &SinkContext,
    ) -> Result<()> {
        let file_descriptor = open_file(path, mode.clone()).await?;

        match mode {
            FileMode::Read => {
                self.files_to_read.send(file_descriptor).await?;
            }
            FileMode::WriteAppend | FileMode::WriteTruncate | FileMode::Overwrite => {
                self.files_to_write
                    .insert(handle.to_string(), file_descriptor);
            }
        }

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        // NOTE: The read file is closed automagically on EOF

        if let Some((_handle, _file)) = self.files_to_write.remove(handle) {
            // file.file.close().await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        // Nothing happens in connect, the files are opened when we receive events
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = event.ingest_ns;

        for (value, meta) in event.value_meta_iter() {
            let (mut file, mut handle) = if let Some(command) = meta.get("command") {
                let path = command
                    .get("path")
                    .as_str()
                    .ok_or_else(|| Error::from("No path provided"))?
                    .to_string();
                let mode: FileMode = structurize(
                    command
                        .get("mode")
                        .ok_or_else(|| Error::from("No mode provided"))?
                        .clone(),
                )?;

                (open_file(&path, mode).await?, None)
            } else {
                let handle = meta.get("handle").as_str().map(ToString::to_string);

                let handle = handle
                    .or_else(|| self.default_handle.clone())
                    .ok_or_else(|| Error::from("No handle provided."))?;

                let (_, file) = self
                    .files_to_write
                    .remove(handle.as_str())
                    .ok_or_else(|| Error::from("No file with that handle"))?;

                (file, Some(handle))
            };

            let data = serializer.serialize(value, ingest_ns)?;
            for chunk in data {
                if let Err(e) = file.file.as_mut().ok_or("no file")?.write_all(&chunk).await {
                    error!("{ctx} Error writing to file: {e}");
                    if let Some(handle) = &handle {
                        self.files_to_write.remove(handle.as_str());
                    }
                    if let Some(handle) = handle.take() {
                        self.files_to_write.insert(handle, file);
                    }
                    ctx.notifier().connection_lost().await?;
                    return Err(e.into());
                }
            }
            if let Err(e) = file.file.as_mut().ok_or("no file")?.flush().await {
                error!("{ctx} Error flushing file: {e}");
                if let Some(handle) = &handle {
                    self.files_to_write.remove(handle.as_str());
                }
                if let Some(handle) = handle.take() {
                    self.files_to_write.insert(handle, file);
                }

                ctx.notifier().connection_lost().await?;
                return Err(e.into());
            }
            if let Some(handle) = handle.take() {
                self.files_to_write.insert(handle, file);
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

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        if let Some(file) = self.file.take() {
            if let Err(e) = file.file.ok_or("no file")?.sync_all().await {
                error!("{} Error flushing file: {}", &ctx, e);
            }
        }
        Ok(())
    }
}
