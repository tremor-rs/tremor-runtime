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

use crate::connectors::prelude::*;
use crate::connectors::utils::url::{Defaults, Url};
use crate::errors::{Error, Result};
use async_std::net::{TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "UPPERCASE")]
pub(crate) struct UdpSocketOptions {
    #[serde(default = "default_false")]
    so_reuseport: bool,
    #[serde(default = "default_true")]
    so_reuseaddr: bool,
    // TODO: add more options
}

impl Default for UdpSocketOptions {
    fn default() -> Self {
        Self {
            so_reuseport: false,
            so_reuseaddr: true,
        }
    }
}

impl UdpSocketOptions {
    /// apply the given config to `sock`
    fn apply_to(&self, sock: &Socket) -> Result<()> {
        sock.set_reuse_port(self.so_reuseport)?;
        sock.set_reuse_address(self.so_reuseaddr)?;
        Ok(())
    }
}

pub(crate) async fn udp_socket<D: Defaults>(
    url: &Url<D>,
    options: &UdpSocketOptions,
) -> Result<UdpSocket> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let mut last_err = None;
    for addr in host_port.to_socket_addrs().await? {
        let sock_addr = SockAddr::from(addr);
        // the bind operation is also not awaited or anything in `UdpSocket::bind`, so this is fine here
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| format!("Invalid address {}:{}", host_port.0, host_port.1))?;
        let sock = Socket::new(
            Domain::for_address(socket_addr),
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;

        // apply socket options
        options.apply_to(&sock)?;

        match sock.bind(&sock_addr) {
            Ok(()) => {
                let socket: std::net::UdpSocket = sock.into();
                return Ok(UdpSocket::from(socket)); // here the socket is set to non-blocking
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or_else(
        || Error::from("could not resolve to any addresses"),
        Error::from,
    ))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "UPPERCASE")]
pub(crate) struct TcpSocketOptions {
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
    fn apply_to(&self, sock: &Socket) -> Result<()> {
        sock.set_reuse_port(self.so_reuseport)?;
        sock.set_reuse_address(self.so_reuseaddr)?;
        sock.set_nodelay(self.tcp_nodelay)?;
        Ok(())
    }
}

pub(crate) async fn tcp_server_socket<D: Defaults>(
    url: &Url<D>,
    backlog: i32,
    options: &TcpSocketOptions,
) -> Result<TcpListener> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let mut last_err = None;
    for addr in host_port.to_socket_addrs().await? {
        let sock_addr = SockAddr::from(addr);
        // the bind operation is also not awaited or anything in `UdpSocket::bind`, so this is fine here
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| format!("Invalid address {}:{}", host_port.0, host_port.1))?;
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
                return Ok(TcpListener::from(socket)); // here the socket is set to non-blocking
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or_else(
        || Error::from("could not resolve to any addresses"),
        Error::from,
    ))
}

pub(crate) async fn tcp_client_socket<D: Defaults>(
    url: &Url<D>,
    options: &TcpSocketOptions,
) -> Result<TcpStream> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let mut last_err = None;
    for addr in host_port.to_socket_addrs().await? {
        let sock_addr = SockAddr::from(addr);
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| format!("Invalid address {}:{}", host_port.0, host_port.1))?;
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
                return Ok(TcpStream::from(socket)); // here the socket is set to non-blocking
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or_else(
        || Error::from("could not resolve to any addresses"),
        Error::from,
    ))
}
