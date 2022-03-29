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

mod client;
mod server;

use crate::{
    connectors::utils::tls::{load_server_config, TLSServerConfig},
    errors::{Error, Result},
};
use async_std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    prelude::*,
    sync::Arc,
    task::{self, JoinHandle},
};
use async_tls::TlsAcceptor;
use std::net::{Shutdown, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::setup_for_tls;

struct EchoServer {
    addr: String,
    i_shall_run: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<()>>>,
    tls: bool,
}

impl EchoServer {
    fn new(addr: String, tls: bool) -> Self {
        Self {
            addr,
            i_shall_run: Arc::new(AtomicBool::new(true)),
            handle: None,
            tls,
        }
    }

    pub(crate) async fn handle_conn<Stream: Read + Write + Send + Sync + Unpin>(
        mut stream: Stream,
        tcp: TcpStream,
        addr: SocketAddr,
        i_shall_continue: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut buf = vec![0_u8; 1024];
        while i_shall_continue.load(Ordering::Acquire) {
            match stream
                .read(&mut buf)
                .timeout(Duration::from_millis(100))
                .await
            {
                Err(_) => continue,
                Ok(Ok(0)) => {
                    info!("[ECHO SERVER] EOF");
                    break;
                }
                Ok(Ok(bytes_read)) => {
                    debug!("[ECHO SERVER] Received {bytes_read} bytes.");
                    buf.truncate(bytes_read);
                    stream.write_all(&buf).await.map_err(|e| {
                        Error::from(format!("Error writing to tcp connection: {e}"))
                    })?;
                }
                Ok(Err(e)) => {
                    error!("[ECHO SERVER] Error: {e}");
                    return Err(Error::from(format!(
                        "Error reading from tcp connection: {e}"
                    )));
                }
            }
        }
        debug!("[ECHO SERVER] Closing connection from {addr}");
        tcp.shutdown(Shutdown::Both)
            .map_err(|e| Error::from(format!("Error shutting down the connection: {e}.")))?;
        Ok(())
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        let server_run = self.i_shall_run.clone();
        let mut conns = vec![];
        let addr = self.addr.clone();
        let tls_config = if self.tls {
            setup_for_tls();
            Some(load_server_config(&TLSServerConfig {
                cert: "./tests/localhost.cert".into(),
                key: "./tests/localhost.key".into(),
            })?)
        } else {
            None
        };
        self.handle = Some(task::spawn::<_, Result<()>>(async move {
            let listener = TcpListener::bind(addr.clone()).await?;
            debug!("[ECHO SERVER] Listening on: {}", addr.clone());
            while server_run.load(Ordering::Acquire) {
                let tls_acceptor = tls_config.as_ref().map(|c| TlsAcceptor::from(c.clone()));

                match listener.accept().timeout(Duration::from_millis(100)).await {
                    Ok(Ok((stream, addr))) => {
                        let tcp = stream.clone();
                        debug!("[ECHO SERVER] New connection from {addr}");
                        let conn = if let Some(tls_acceptor) = tls_acceptor {
                            let tls_stream = tls_acceptor.accept(stream).await?;
                            task::spawn(Self::handle_conn(
                                tls_stream,
                                tcp,
                                addr,
                                server_run.clone(),
                            ))
                        } else {
                            task::spawn(Self::handle_conn(stream, tcp, addr, server_run.clone()))
                        };
                        conns.push(conn);
                    }
                    Ok(Err(e)) => {
                        error!("[ECHO SERVER] Error: {e}");
                        break;
                    }
                    Err(_) => continue,
                }
            }
            for res in futures::future::join_all(conns).await {
                res?;
            }
            info!("[ECHO SERVER] stopped.");
            Ok(())
        }));
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<()> {
        self.i_shall_run.store(false, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.await?;
        }
        Ok(())
    }
}
