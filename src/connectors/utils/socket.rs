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

use crate::connectors::utils::url::{Defaults, Url};
use crate::errors::Result;
use async_std::net::{ToSocketAddrs, UdpSocket};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "UPPERCASE")]
pub(crate) struct UdpSocketOptions {
    #[serde(default)]
    so_reuseport: bool,
    // TODO: add more options
}

impl UdpSocketOptions {
    /// apply the given config to `sock`
    fn apply_to(&self, sock: &Socket) -> Result<()> {
        sock.set_reuse_port(self.so_reuseport)?;
        Ok(())
    }
}

pub(crate) async fn udp_socket<D: Defaults>(
    url: &Url<D>,
    options: Option<&UdpSocketOptions>,
) -> Result<UdpSocket> {
    let host_port = (url.host_or_local(), url.port_or_dflt());
    let addr = host_port
        .to_socket_addrs()
        .await?
        .next()
        .ok_or_else(|| format!("Invalid address {}:{}", host_port.0, host_port.1))?;
    {
        let sock_addr = SockAddr::from(addr);
        let socket_addr = sock_addr
            .as_socket()
            .ok_or_else(|| format!("Invalid address {}:{}", host_port.0, host_port.1))?;
        let sock = Socket::new(
            Domain::for_address(socket_addr),
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        // apply reuseport socket action
        if let Some(options) = options {
            options.apply_to(&sock);
        }
        sock.bind(&sock_addr)?; // the bind operation is also not awaited or anything in `UdpSocket::bind`, so this is fine here
        let socket: std::net::UdpSocket = sock.into();
        Ok(UdpSocket::from(socket)) // here the socket is set to non-blocking
    }
}
