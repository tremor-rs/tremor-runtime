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

#![allow(clippy::doc_markdown)]
//! The [`udp_server`](#udp_server) and [`udp_client`](#udp_client) connectors allow UDP based datagram clients and servers to be integrated with tremor.
//!
//! ## `udp_server`
//!
//! The `udp_server` binds to the host and port given in `url` and listens on incoming UDP packets.
//! Incoming UDP packets are being received into a local buffer of `buf_size` bytes, which determines the maximum packet size. Each UDP packet will be deserialized by the preprocessors and the codec.
//!
//! This connector can only be used to receive events, thus pipelines can only be connected to the `out` and `err` ports.
//!
//! Events coming from the `udp_server` connector do not have any metadata associated with them. The [`tremor::origin::scheme()`](../stdlib/tremor/origin.md#scheme) function can be used and checked for equality with `"udp-server"` to determine if an event is coming from the `udp_server` connector.
//!
//! ### Configuration
//!
//! | Option           | Description                                                                                        | Type             | Required | Default value                                                                |
//! |------------------|----------------------------------------------------------------------------------------------------|------------------|----------|------------------------------------------------------------------------------|
//! | `url`            | The host and port to bind to and listen on for incoming packets.                                   | string           | yes      |                                                                              |
//! | `buf_size`       | UDP receive buffer size. This should be greater than or equal to the expected maximum packet size. | positive integer | no       | 8192                                                                         |
//! | `socket_options` | See [UDP socket options](./index.md#udp-socket-options).                            | record           | no       | See [UDP socket options defaults](./index#udp-socket-options) |
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector `udp-in` from udp_server
//! with
//!   codec = "string",
//!   preprocessors = [
//!     "separate"
//!   ]
//!   config = {
//!     "url": "localhost:4242",
//!     "buf_size": 4096,
//!     "socket_options": {
//!       "SO_REUSEPORT": true
//!     }
//!   }
//! end;
//! ```
//!
//! ## `udp_client`
//!
//! The UDP client will open a UDP socket to write data to the given host and port configured in `url`. It will write the event payload, processed by the configured codec and postprocessors, out to the socket.
//!
//! ### Configuration
//!
//! | Option           | Description                                                             | Type   | Required | Default value                                                                                |
//! |------------------|-------------------------------------------------------------------------|--------|----------|----------------------------------------------------------------------------------------------|
//! | `url`            | The host and port to connect to.                                        | string | yes      |                                                                                              |
//! | `bind`           | The host and port to bind to prior to connecting                        | string | no       | "0.0.0.0:0" if the connect url resolves to an IPv4 address, "[::]:0" if it resolves to IPv6. |
//! | `socket_options` | See [UDP socket options](./index.md#udp-socket-options). | record | no       | See [UDP socket options defaults](./index#udp-socket-options)                 |
//!
//! The `udp` client, by default binds to `0.0.0.0:0` allowing to send to all interfaces of the system running tremor and picking a random port. This can be overwritten adding `"bind": "<ip>:<port>"` to the `config`.
//!
//! :::warn
//! If you are hardening an installation it might make sense to limit the interfaces a udp client can send to by specifying the `"bind"` config.
//! :::
//!
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector `udp-out` from udp_client
//! with
//!   codec = "yaml",
//!   postprocessors = ["base64"],
//!   config = {
//!     "url": "localhost:4242",
//!     "bind": "127.0.0.1:65535",
//!     "socket_options: {
//!       "SO_REUSEPORT": true
//!     }
//!   }
//! end;
//! ```

pub(crate) mod client;
pub(crate) mod server;

use crate::prelude::{default_false, default_true, Defaults};
use crate::utils::socket::Error;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::{lookup_host, UdpSocket};
use tremor_common::url::Url;

pub(crate) struct UdpDefaults;
impl Defaults for UdpDefaults {
    const SCHEME: &'static str = "udp";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 0;
}

const UDP_IPV4_UNSPECIFIED: &str = "0.0.0.0:0";
const UDP_IPV6_UNSPECIFIED: &str = "[::]:0";

pub(crate) async fn udp_socket<D: Defaults>(
    url: &Url<D>,
    options: &UdpSocketOptions,
) -> Result<UdpSocket, Error> {
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
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;

        // apply socket options
        options.apply_to(&sock)?;

        match sock.bind(&sock_addr) {
            Ok(()) => {
                let socket: std::net::UdpSocket = sock.into();
                socket.set_nonblocking(true)?;
                return Ok(UdpSocket::from_std(socket)?);
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.map_or_else(|| Error::CouldNotResolve.into(), Error::from))
}

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
    fn apply_to(&self, sock: &Socket) -> Result<(), Error> {
        sock.set_reuse_port(self.so_reuseport)?;
        sock.set_reuse_address(self.so_reuseaddr)?;
        Ok(())
    }
}
