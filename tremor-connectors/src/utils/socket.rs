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

use crate::prelude::*;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::lookup_host;
use tokio::net::{TcpListener, TcpStream};
use tremor_common::url::{Defaults, Url};

/// Generic socket errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No socket
    #[error("no socket")]
    NoSocket,
    /// Could not resolve to any addresses
    #[error("could not resolve to any addresses")]
    CouldNotResolve,
    /// Invalid address
    #[error("invalid address {0}:{1}")]
    InvalidAddress(String, u16),
    /// IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration options for TCP sockets
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "UPPERCASE")]
pub struct TcpSocketOptions {
    #[serde(default)]
    so_reuseport: bool,
    #[serde(default = "default_true")]
    so_reuseaddr: bool,
    #[serde(default = "default_true")]
    tcp_nodelay: bool,
    // TODO: add more options
}

impl Default for TcpSocketOptions {
    fn default() -> Self {
        Self {
            so_reuseport: false,
            so_reuseaddr: true,
            tcp_nodelay: true,
        }
    }
}

impl TcpSocketOptions {
    fn apply_to(&self, sock: &Socket) -> Result<(), Error> {
        sock.set_reuse_port(self.so_reuseport)?;
        sock.set_reuse_address(self.so_reuseaddr)?;
        sock.set_nodelay(self.tcp_nodelay)?;
        Ok(())
    }
}

/// Create a TCP server socket
/// This function will try to bind to the given URL and start listening for incoming connections
/// # Errors
/// This function will return an error if it could not resolve the given URL to any addresses or if it could not bind to any of the resolved addresses
pub async fn tcp_server<D: Defaults>(
    url: &Url<D>,
    backlog: i32,
    options: &TcpSocketOptions,
) -> Result<TcpListener, Error> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let mut last_err = None;
    for addr in lookup_host(host_port).await? {
        let sock_addr = SockAddr::from(addr);
        // the bind operation is also not awaited or anything in `UdpSocket::bind`, so this is fine here
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| Error::InvalidAddress(host_port.0.to_string(), host_port.1))?;
        let sock = Socket::new(
            Domain::for_address(socket_addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        // apply socket options
        options.apply_to(&sock)?;

        match sock.bind(&sock_addr) {
            Ok(()) => {
                sock.listen(backlog)?;
                let socket: std::net::TcpListener = sock.into();
                // here the socket is set to non-blocking
                socket.set_nonblocking(true)?;
                return Ok(TcpListener::from_std(socket)?);
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or(Error::CouldNotResolve, Error::from))
}

/// Connect to a TCP server
/// This function will try to connect to the given URL
/// # Errors
/// This function will return an error if it could not resolve the given URL to any addresses or if it could not connect to any of the resolved addresses
pub async fn tcp_client<D: Defaults>(
    url: &Url<D>,
    options: &TcpSocketOptions,
) -> Result<TcpStream, Error> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let mut last_err = None;
    for addr in lookup_host(host_port).await? {
        let sock_addr = SockAddr::from(addr);
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| Error::InvalidAddress(host_port.0.to_string(), host_port.1))?;
        let sock = Socket::new(
            Domain::for_address(socket_addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        // apply socket options
        options.apply_to(&sock)?;

        match sock.connect(&sock_addr) {
            Ok(()) => {
                let socket: std::net::TcpStream = sock.into();
                // here the socket is set to non-blocking
                socket.set_nonblocking(true)?;
                return Ok(TcpStream::from_std(socket)?);
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or(Error::CouldNotResolve, Error::from))
}
