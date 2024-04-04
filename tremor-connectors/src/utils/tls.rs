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

//! TLS utilities

use futures::Future;
use hyper::server::{
    accept::Accept,
    conn::{AddrIncoming, AddrStream},
};
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys, Item};
use std::{
    io::{self, BufReader},
    net::SocketAddr,
    pin::Pin,
    task::{ready, Context, Poll},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;

lazy_static::lazy_static! {
    static ref SYSTEM_ROOT_CERTS: RootCertStore = {
        let mut roots = RootCertStore::empty();
        // ALLOW: this is expected to panic if we cannot load system certificates
        for cert in load_native_certs().expect("Unable to load system TLS certificates.") {
            roots
                .add(&Certificate(cert.0))
                // ALLOW: this is expected to panic if we cannot load system certificates
                .expect("Unable to add root TLS certificate to RootCertStore");
        }
        roots
    };
}

/// TLS Server Configuration
#[derive(Debug, Clone, Deserialize)]
pub struct TLSServerConfig {
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

impl TLSServerConfig {
    /// creates a new server config
    #[must_use]
    pub fn new<P1, P2>(cert: P1, key: P2) -> Self
    where
        P1: Into<PathBuf>,
        P2: Into<PathBuf>,
    {
        TLSServerConfig {
            cert: cert.into(),
            key: key.into(),
        }
    }

    /// Create a new server config from the `TLSServerConfig`
    /// # Errors
    /// if the cert or key is invalid
    pub fn to_server_config(&self) -> Result<ServerConfig, Error> {
        let certs = load_certs(&self.cert)?;

        let key = load_keys(&self.key)?;

        let server_config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            // set this server to use one cert together with the loaded private key
            .with_single_cert(certs, key)?;

        Ok(server_config)
    }
}

/// TLS Client Configuration
#[derive(Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct TLSClientConfig {
    /// Path to the pem-encoded certificate file of the CA to use for verifying the servers certificate
    pub(crate) cafile: Option<PathBuf>,
    /// The DNS domain used to verify the server's certificate. If not provided the domain from the connection URL will be used.
    pub(crate) domain: Option<String>,
    /// Path to the pem-encoded certificate (-chain) to use for TLS with client-side certificate
    pub(crate) cert: Option<PathBuf>,
    /// Path to the private key to use for TLS with client-side certificate
    pub(crate) key: Option<PathBuf>,
}

impl TLSClientConfig {
    /// creates a new client config
    #[must_use]
    pub fn new(
        cafile: Option<PathBuf>,
        domain: Option<String>,
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
    ) -> Self {
        TLSClientConfig {
            cafile,
            domain,
            cert,
            key,
        }
    }

    /// Create a new client connector from the `TLSClientConfig`
    /// if we have a cafile configured, we only load it, and no other ca certificates
    /// if there is no cafile configured, we load the default webpki-roots from Mozilla
    ///
    /// # Errors
    /// if the cafile is invalid

    pub fn to_client_connector(&self) -> Result<TlsConnector, Error> {
        let tls_config = self.to_client_config()?;
        Ok(TlsConnector::from(Arc::new(tls_config)))
    }
    /// Create a new client config from the `TLSClientConfig`
    /// if we have a cafile configured, we only load it, and no other ca certificates
    /// if there is no cafile configured, we load the default webpki-roots from Mozilla
    ///
    /// # Errors
    /// if the cafile is invalid
    pub fn to_client_config(&self) -> Result<ClientConfig, Error> {
        let roots = if let Some(cafile) = self.cafile.as_ref() {
            let mut roots = RootCertStore::empty();
            let certfile = tremor_common::file::open(cafile)?;
            let mut reader = BufReader::new(certfile);

            let cert = rustls_pemfile::read_one(&mut reader)?.and_then(|item| match item {
                Item::X509Certificate(cert) => Some(Certificate(cert)),
                _ => None,
            });
            cert.and_then(|cert| roots.add(&cert).ok())
                .ok_or_else(|| Error::InvalidCertificate(cafile.to_string_lossy().to_string()))?;
            roots
        } else {
            SYSTEM_ROOT_CERTS.clone()
        };

        let tls_config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots);

        // load client certificate stuff
        if let (Some(cert), Some(key)) = (self.cert.as_ref(), self.key.as_ref()) {
            let cert = load_certs(cert)?;
            let key = load_keys(key)?;
            Ok(tls_config.with_client_auth_cert(cert, key)?)
        } else {
            Ok(tls_config.with_no_client_auth())
        }
    }
}

/// TLS Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid certificate
    #[error("Invalid certificate file: {0}")]
    InvalidCertificate(String),
    /// Certificate not found
    #[error("Certificate file does not exist: {0}")]
    CertificateNotFound(String),
    /// Invalid private key
    #[error("Invalid private key file: {0}")]
    InvalidPrivateKey(String),
    /// Tremor Common error
    #[error(transparent)]
    TremorCommon(#[from] tremor_common::Error),
    /// TLS error
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    /// IO error
    #[error(transparent)]
    IO(#[from] io::Error),
}

/// Load the passed certificates file
fn load_certs(path: &Path) -> Result<Vec<Certificate>, Error> {
    let certfile = tremor_common::file::open(path)?;
    let mut reader = BufReader::new(certfile);
    certs(&mut reader)
        .map_err(|_| Error::InvalidCertificate(path.to_string_lossy().to_string()))
        .and_then(|certs| {
            if certs.is_empty() {
                Err(Error::CertificateNotFound(
                    path.to_string_lossy().to_string(),
                ))
            } else {
                Ok(certs.into_iter().map(Certificate).collect())
            }
        })
}

/// Load the passed private key file
fn load_keys(path: &Path) -> Result<PrivateKey, Error> {
    // prefer to load pkcs8 keys
    // this will only error if we have invalid pkcs8 key base64 or we couldnt read the file.
    let keyfile = tremor_common::file::open(path)?;
    let mut reader = BufReader::new(keyfile);

    let certs = pkcs8_private_keys(&mut reader)
        .map_err(|_e| Error::InvalidPrivateKey(path.to_string_lossy().to_string()))?;
    let mut keys: Vec<PrivateKey> = certs.into_iter().map(PrivateKey).collect();

    // only attempt to load as RSA keys if file has no pkcs8 keys
    if keys.is_empty() {
        let keyfile = tremor_common::file::open(path)?;
        let mut reader = BufReader::new(keyfile);
        keys = rsa_private_keys(&mut reader)
            .map_err(|_e| Error::InvalidPrivateKey(path.to_string_lossy().to_string()))?
            .into_iter()
            .map(PrivateKey)
            .collect();
    }

    keys.into_iter()
        .next()
        .ok_or_else(|| Error::InvalidPrivateKey(path.to_string_lossy().to_string()))
}

// This is a copy of https://github.com/rustls/hyper-rustls/blob/main/examples/server.rs
// for some reason they don't provide it as part of the library
enum State {
    Handshaking(tokio_rustls::Accept<AddrStream>),
    Streaming(tokio_rustls::server::TlsStream<AddrStream>),
}

/// `tokio_rustls::server::TlsStream` doesn't expose constructor methods,
/// so we have to `TlsAcceptor::accept` and handshake to have access to it
/// `TlsStream` implements AsyncRead/AsyncWrite handshaking `tokio_rustls::Accept` first
pub struct Stream {
    state: State,
}

impl Stream {
    fn new(stream: AddrStream, config: Arc<ServerConfig>) -> Self {
        let accept = tokio_rustls::TlsAcceptor::from(config).accept(stream);
        Self {
            state: State::Handshaking(accept),
        }
    }
    pub(crate) fn remote_addr(&self) -> Option<SocketAddr> {
        match self.state {
            State::Handshaking(ref accept) => accept
                .get_ref()
                .map(hyper::server::conn::AddrStream::remote_addr),
            State::Streaming(ref stream) => Some(stream.get_ref().0.remote_addr()),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        let pin = self.get_mut();
        match pin.state {
            State::Handshaking(ref mut accept) => match ready!(Pin::new(accept).poll(cx)) {
                Ok(mut stream) => {
                    let result = Pin::new(&mut stream).poll_read(cx, buf);
                    pin.state = State::Streaming(stream);
                    result
                }
                Err(err) => Poll::Ready(Err(err)),
            },
            State::Streaming(ref mut stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let pin = self.get_mut();
        match pin.state {
            State::Handshaking(ref mut accept) => match ready!(Pin::new(accept).poll(cx)) {
                Ok(mut stream) => {
                    let result = Pin::new(&mut stream).poll_write(cx, buf);
                    pin.state = State::Streaming(stream);
                    result
                }
                Err(err) => Poll::Ready(Err(err)),
            },
            State::Streaming(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.state {
            State::Handshaking(_) => Poll::Ready(Ok(())),
            State::Streaming(ref mut stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.state {
            State::Handshaking(_) => Poll::Ready(Ok(())),
            State::Streaming(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Acceptor for TLS connections
pub struct Acceptor {
    config: Arc<ServerConfig>,
    incoming: AddrIncoming,
}

impl Acceptor {
    /// Create a new Acceptor
    pub fn new(config: Arc<ServerConfig>, incoming: AddrIncoming) -> Acceptor {
        Acceptor { config, incoming }
    }
}

impl Accept for Acceptor {
    type Conn = Stream;
    type Error = io::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Self::Conn, Self::Error>>> {
        let pin = self.get_mut();
        match ready!(Pin::new(&mut pin.incoming).poll_accept(cx)) {
            Some(Ok(sock)) => Poll::Ready(Some(Ok(Stream::new(sock, pin.config.clone())))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::harness::setup_for_tls;

    use super::*;

    #[test]
    fn load_certs_invalid() -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"Brueghelflinsch\n")?;
        let path = file.into_temp_path();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_certs_empty() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_invalid() -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(
            b"-----BEGIN PRIVATE KEY-----\nStrumpfenpfart\n-----END PRIVATE KEY-----\n",
        )?;
        let path = file.into_temp_path();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_empty() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[test]
    fn client_config() -> anyhow::Result<()> {
        setup_for_tls();

        let tls_config = TLSClientConfig {
            cafile: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            domain: Some("hostenschmirtz".to_string()),
            cert: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            key: Some(Path::new("./tests/localhost.key").to_path_buf()),
        };
        let client_config = tls_config.to_client_config()?;
        assert!(client_config.client_auth_cert_resolver.has_certs());
        Ok(())
    }
}
