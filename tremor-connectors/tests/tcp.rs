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

#![cfg(feature = "integration-tests-tcp")]
mod tcp {
    mod client;
    mod pause_resume;
    mod server;
}

use anyhow::{anyhow, bail, Result};
use log::{debug, error, info};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::{self, JoinHandle},
};
use tremor_connectors::utils::tls::TLSServerConfig;
use tremor_connectors_test_helpers::setup_for_tls;

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

    pub(crate) async fn handle_conn<Stream: AsyncRead + AsyncWrite + Send + Sync + Unpin>(
        mut stream: Stream,
        addr: SocketAddr,
        i_shall_continue: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        let mut buf = vec![0_u8; 1024];
        while i_shall_continue.load(Ordering::Acquire) {
            match timeout(Duration::from_millis(100), stream.read(&mut buf)).await {
                Err(_) => continue,
                Ok(Ok(0)) => {
                    info!("[ECHO SERVER] EOF");
                    break;
                }
                Ok(Ok(bytes_read)) => {
                    debug!("[ECHO SERVER] Received {bytes_read} bytes.");
                    buf.truncate(bytes_read);
                    stream
                        .write_all(&buf)
                        .await
                        .map_err(|e| anyhow!("Error writing to tcp connection: {e}"))?;
                }
                Ok(Err(e)) => {
                    error!("[ECHO SERVER] Error: {e}");
                    bail!("Error reading from tcp connection: {e}");
                }
            }
        }
        debug!("[ECHO SERVER] Closing connection from {addr}");
        Ok(())
    }

    pub(crate) fn run(&mut self) -> anyhow::Result<()> {
        let server_run = self.i_shall_run.clone();
        let mut conns = vec![];
        let addr = self.addr.clone();
        let tls_config = if self.tls {
            setup_for_tls();
            Some(
                TLSServerConfig::new("./tests/localhost.cert", "./tests/localhost.key")
                    .to_server_config()?,
            )
        } else {
            None
        };
        self.handle = Some(task::spawn(async move {
            let listener = TcpListener::bind(addr.clone()).await?;
            debug!("[ECHO SERVER] Listening on: {}", addr.clone());
            while server_run.load(Ordering::Acquire) {
                let tls_acceptor = tls_config
                    .as_ref()
                    .map(|c| tokio_rustls::TlsAcceptor::from(Arc::new(c.clone())));

                match timeout(Duration::from_millis(100), listener.accept()).await {
                    Ok(Ok((tcp, addr))) => {
                        debug!("[ECHO SERVER] New connection from {addr}");
                        let conn = if let Some(tls_acceptor) = tls_acceptor {
                            match tls_acceptor.accept(tcp).await {
                                Ok(tls_stream) => task::spawn(Self::handle_conn(
                                    tls_stream,
                                    addr,
                                    server_run.clone(),
                                )),
                                Err(e) => {
                                    error!("[ECHO SERVER] Error accepting TLS Connection: {e}");
                                    continue;
                                }
                            }
                        } else {
                            task::spawn(Self::handle_conn(tcp, addr, server_run.clone()))
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
                res??;
            }
            info!("[ECHO SERVER] stopped.");
            Result::Ok(())
        }));
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> anyhow::Result<()> {
        self.i_shall_run.store(false, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.await??;
        }
        Ok(())
    }
}
