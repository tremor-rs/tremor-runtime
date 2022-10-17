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

use crate::connectors::prelude::SinkContext;
use crate::connectors::sink::prelude::*;
use tokio::fs::OpenOptions;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum FileMode {
    /// read from file
    Read,
    /// append to the file
    WriteAppend,
    /// truncate the file to 0 bytes and then write to it
    WriteTruncate,
    /// just write to it and overwrite existing contents, do not truncate
    Overwrite,
}

impl FileMode {
    pub fn as_open_options(&self) -> OpenOptions {
        let mut o = OpenOptions::new();
        match self {
            Self::Read => {
                o.read(true);
            }
            Self::WriteAppend => {
                o.create(true).write(true).append(true);
            }
            Self::WriteTruncate => {
                o.create(true).write(true).truncate(true);
            }
            Self::Overwrite => {
                o.create(true).write(true);
            }
        }
        o
    }
}

#[async_trait::async_trait]
pub(crate) trait FileIo {
    async fn open(
        &mut self,
        path: &str,
        handle: &str,
        mode: FileMode,
        ctx: &SinkContext,
    ) -> Result<()>;
    async fn close(&mut self, handle: &str, ctx: &SinkContext) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait SocketServer {
    async fn listen(&mut self, address: &str, handle: &str, ctx: &SinkContext) -> Result<()>;
    async fn close(&mut self, handle: &str, ctx: &SinkContext) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait SocketClient {
    async fn connect_socket(
        &mut self,
        address: &str,
        handle: &str,
        ctx: &SinkContext,
    ) -> Result<()>;
    async fn close(&mut self, handle: &str, ctx: &SinkContext) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait QueueSubscriber {
    async fn subscribe(
        &mut self,
        topics: Vec<String>,
        handle: &str,
        ctx: &SinkContext,
    ) -> Result<()>;
    async fn unsubscribe(&mut self, handle: &str, ctx: &SinkContext) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait DatabaseWriter {
    async fn open_table(&mut self, table: &str, handle: &str, ctx: &SinkContext) -> Result<()>;

    async fn close_table(&mut self, handle: &str, ctx: &SinkContext) -> Result<()>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "file_io")]
pub enum FileIoCommand {
    Open {
        path: String,
        handle: String,
        mode: FileMode,
    },
    Close {
        handle: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "socket_server")]
pub enum SocketServerCommand {
    Listen { address: String, handle: String },
    Close { handle: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "socket_client")]
pub enum SocketClientCommand {
    Connect { address: String, handle: String },
    Close { handle: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "queue_subscriber")]
pub enum QueueSubscriberCommand {
    Subscribe { topics: Vec<String>, handle: String },
    Unsubscribe { topic: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "database_writer")]
pub enum DatabaseWriterCommand {
    OpenTable { table: String, handle: String },
    CloseTable { handle: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", rename = "command")]
pub enum Command {
    FileIo(FileIoCommand),
    SocketServer(SocketServerCommand),
    SocketClient(SocketClientCommand),
    QueueSubscriber(QueueSubscriberCommand),
    DatabaseWriter(DatabaseWriterCommand),
}

impl FileIoCommand {
    async fn execute<T: Sink + FileIo + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            FileIoCommand::Open { handle, mode, path } => {
                sink.open(&path, &handle, mode, ctx).await
            }
            FileIoCommand::Close { handle } => FileIo::close(sink, &handle, ctx).await,
        }
    }
}

impl SocketServerCommand {
    async fn execute<T: Sink + SocketServer + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            SocketServerCommand::Listen { handle, address } => {
                sink.listen(&address, &handle, ctx).await
            }
            SocketServerCommand::Close { handle } => SocketServer::close(sink, &handle, ctx).await,
        }
    }
}

impl SocketClientCommand {
    async fn execute<T: Sink + SocketClient + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            SocketClientCommand::Connect { handle, address } => {
                SocketClient::connect_socket(sink, &address, &handle, ctx).await
            }
            SocketClientCommand::Close { handle } => SocketClient::close(sink, &handle, ctx).await,
        }
    }
}

impl QueueSubscriberCommand {
    async fn execute<T: Sink + QueueSubscriber + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            QueueSubscriberCommand::Subscribe { handle, topics } => {
                QueueSubscriber::subscribe(sink, topics, &handle, ctx).await
            }
            QueueSubscriberCommand::Unsubscribe { topic } => {
                QueueSubscriber::unsubscribe(sink, &topic, ctx).await
            }
        }
    }
}

impl DatabaseWriterCommand {
    async fn execute<T: Sink + DatabaseWriter + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            DatabaseWriterCommand::OpenTable { handle, table } => {
                DatabaseWriter::open_table(sink, &table, &handle, ctx).await
            }
            DatabaseWriterCommand::CloseTable { handle } => {
                DatabaseWriter::close_table(sink, &handle, ctx).await
            }
        }
    }
}

impl Command {
    pub(crate) async fn execute<T: Sink + ?Sized>(
        self,
        sink: &mut T,
        ctx: &SinkContext,
    ) -> Result<()> {
        match self {
            Command::FileIo(command) => command.execute(sink, ctx).await,
            Command::SocketServer(command) => command.execute(sink, ctx).await,
            Command::SocketClient(command) => command.execute(sink, ctx).await,
            Command::QueueSubscriber(command) => command.execute(sink, ctx).await,
            Command::DatabaseWriter(command) => command.execute(sink, ctx).await,
        }
    }
}
